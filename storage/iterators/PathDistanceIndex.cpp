#include "PathDistanceIndex.h"

#include <algorithm>
#include <math.h>

#include "PartDirectory.h"
#include "PathHopFilter.h"
#include "datapart/NodeContainer.h"
#include "datapart/NodeRange.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSetHandle.h"
#include "versioning/Tombstones.h"

using namespace db;

namespace {

// One node or edge the search touches costs this many candidate checks of the walk it
// spares: measured with samples/path_bench between 0.22 and 0.35 across graph shapes, and the
// highest of those keeps the gate within 1.4 of each shape's measured break-even either way
constexpr double indexUnitCostInChecks = 0.35;

constexpr size_t fanOutSampleTarget = 4096;

// Fewer than the fan-out sample takes: every node of this one costs an evaluation of the
// query's hop predicate rather than a count of its adjacency
constexpr size_t hopSampleTarget = 1024;

void appendMatching(std::span<const EdgeRecord> edges,
                    std::optional<EdgeTypeID> edgeType,
                    std::vector<NodeID>& candidateNodes,
                    std::vector<EdgeID>& candidateEdges) {
    for (const EdgeRecord& record : edges) {
        if (edgeType && record._edgeTypeID != *edgeType) {
            continue;
        }

        candidateNodes.push_back(record._otherID);
        candidateEdges.push_back(record._edgeID);
    }
}

size_t countMatching(std::span<const EdgeRecord> edges, std::optional<EdgeTypeID> edgeType) {
    if (!edgeType) {
        return edges.size();
    }

    size_t matching = 0;
    for (const EdgeRecord& record : edges) {
        if (record._edgeTypeID == *edgeType) {
            matching++;
        }
    }

    return matching;
}

}

PathDistanceIndex::PathDistanceIndex() {
}

PathDistanceIndex::~PathDistanceIndex() {
}

void PathDistanceIndex::build(const GraphView& view,
                              const LabelSet& endLabels,
                              PathExplorationDir direction,
                              std::optional<EdgeTypeID> edgeType,
                              uint64_t maxHops) {
    const PartDirectory parts(view);

    _distances.assign(parts.getAllocatedNodeCount(), unreachable);
    _reached = 0;

    std::vector<NodeID> frontier;
    collectEnds(parts, endLabels, frontier);

    // A hop the exploration takes forward is walked back here: the distances of the nodes
    // an out-edge leaves grow along in-edges
    const bool walksIns = direction != PathExplorationDir::BACKWARD;
    const bool walksOuts = direction != PathExplorationDir::FORWARD;

    const Tombstones& tombstones = view.tombstones();
    const Tombstones* edgeTombstones = tombstones.hasEdges() ? &tombstones : nullptr;

    std::vector<NodeID> next;

    // A distance saturates at the farthest a byte can hold, so past that one reads "at least
    // that many hops". canReachEndWithin then keeps a node a deeper bound may not be able to
    // use, which costs a walk that never completes; capping the search instead would drop it.
    for (uint64_t level = 1; level <= maxHops && !frontier.empty(); level++) {
        const uint8_t distance = static_cast<uint8_t>(std::min<uint64_t>(level, farthest));
        next.clear();

        for (const NodeID node : frontier) {
            const size_t owner = parts.ownerIndex(node);
            if (owner == parts.size()) {
                continue;
            }

            const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;
            if (walksIns) {
                relax(ownerIndexer.getNodeInEdges(node), distance, edgeType, edgeTombstones, next);
            }
            if (walksOuts) {
                relax(ownerIndexer.getNodeOutEdges(node), distance, edgeType, edgeTombstones, next);
            }

            for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
                const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
                if (walksIns) {
                    relax(patchIndexer.getNodeInEdges(node), distance, edgeType, edgeTombstones, next);
                }
                if (walksOuts) {
                    relax(patchIndexer.getNodeOutEdges(node), distance, edgeType, edgeTombstones, next);
                }
            }
        }

        std::swap(frontier, next);
    }

    _built = true;
}

uint8_t PathDistanceIndex::getDistance(NodeID node) const {
    const size_t index = node.getValue();
    if (index >= _distances.size()) {
        return unreachable;
    }

    return _distances[index];
}

bool PathDistanceIndex::canReachEndWithin(NodeID node, uint64_t hops) const {
    const uint8_t distance = getDistance(node);

    return distance != unreachable && distance <= hops;
}

void PathDistanceIndex::sampleBranching(const PartDirectory& parts,
                                        PathExplorationDir direction,
                                        std::optional<EdgeTypeID> edgeType,
                                        TypeBranching& branching) {
    branching = TypeBranching {};

    const size_t nodeCount = parts.getAllocatedNodeCount();
    if (nodeCount == 0) {
        return;
    }

    const size_t edgeCount = parts.getAllocatedEdgeCount();
    EdgeBranchingCache& cache = parts.getBranchingCache();
    if (cache.lookup(direction, edgeType, nodeCount, edgeCount, branching)) {
        return;
    }

    const size_t stride = std::max<size_t>(1, nodeCount / fanOutSampleTarget);

    size_t sampled = 0;
    size_t reachable = 0;
    double arrivals = 0.0;
    double continuations = 0.0;

    for (size_t node = 0; node < nodeCount; node += stride) {
        const NodeID sample(node);
        const size_t owner = parts.ownerIndex(sample);
        if (owner == parts.size()) {
            continue;
        }

        const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;
        sampled++;

        const size_t outs = countMatching(ownerIndexer.getNodeOutEdges(sample), edgeType);
        const size_t ins = countMatching(ownerIndexer.getNodeInEdges(sample), edgeType);

        // A hop arrives at a node against the direction the next one leaves it by, so a node
        // joins the frontier as often as it has arriving edges and then branches by the ones
        // that continue: weighting each node's branching by its arrivals is what the walk
        // sees, where an average over the graph counts nodes no hop of it ever reaches
        size_t arriving = 0;
        size_t continuing = 0;
        switch (direction) {
            case PathExplorationDir::FORWARD:
                arriving = ins;
                continuing = outs;
            break;
            case PathExplorationDir::BACKWARD:
                arriving = outs;
                continuing = ins;
            break;
            case PathExplorationDir::BOTH:
                arriving = outs + ins;
                continuing = outs + ins;
            break;
        }

        if (arriving == 0) {
            continue;
        }

        reachable++;
        arrivals += static_cast<double>(arriving);
        continuations += static_cast<double>(arriving) * static_cast<double>(continuing);
    }

    if (sampled > 0 && arrivals > 0.0) {
        branching._fanOut = continuations / arrivals;
        branching._supportNodes = static_cast<double>(nodeCount) * static_cast<double>(reachable)
                                  / static_cast<double>(sampled);
    }

    cache.store(direction, edgeType, nodeCount, edgeCount, branching);
}

double PathDistanceIndex::estimatedSearchChecks(const PartDirectory& parts,
                                                PathExplorationDir direction,
                                                std::optional<EdgeTypeID> edgeType,
                                                size_t sourceCount,
                                                uint64_t maxHops) {
    const size_t nodeCount = parts.getAllocatedNodeCount();
    const size_t edgeCount = parts.getAllocatedEdgeCount();
    if (sourceCount == 0 || maxHops == 0 || nodeCount == 0 || edgeCount == 0) {
        return 0.0;
    }

    TypeBranching branching;
    sampleBranching(parts, direction, edgeType, branching);

    const double fanOut = std::max(1.0, branching._fanOut);
    const double support = std::clamp(branching._supportNodes, 1.0, static_cast<double>(nodeCount));
    const uint64_t levelCount = std::min<uint64_t>(maxHops, farthest);

    double candidatesPerSource = 0.0;
    double frontier = 1.0;

    for (uint64_t level = 0; level < levelCount; level++) {
        candidatesPerSource += frontier * fanOut;

        if (frontier >= support) {
            break;
        }

        frontier = std::min(frontier * fanOut, support);
    }

    return static_cast<double>(sourceCount) * candidatesPerSource;
}

double PathDistanceIndex::estimatedEnumerationChecks(const PartDirectory& parts,
                                                     const TypeBranching& branching,
                                                     size_t seedCount,
                                                     uint64_t maxHops,
                                                     double hopPassRate) {
    const size_t nodeCount = parts.getAllocatedNodeCount();
    const size_t edgeCount = parts.getAllocatedEdgeCount();
    if (seedCount == 0 || maxHops == 0 || nodeCount == 0 || edgeCount == 0) {
        return 0.0;
    }

    const double fanOut = std::max(1.0, branching._fanOut);
    const uint64_t levelCount = std::min<uint64_t>(maxHops, farthest);

    double candidatesPerSeed = 0.0;
    double frontier = 1.0;

    for (uint64_t level = 0; level < levelCount; level++) {
        candidatesPerSeed += frontier * fanOut;

        frontier = frontier * fanOut * hopPassRate;
    }

    return static_cast<double>(seedCount) * candidatesPerSeed;
}

double PathDistanceIndex::sampleHopPassRate(const PartDirectory& parts,
                                            PathExplorationDir direction,
                                            std::optional<EdgeTypeID> edgeType,
                                            PathHopFilter& hopFilter) {
    const size_t nodeCount = parts.getAllocatedNodeCount();
    if (nodeCount == 0) {
        return 1.0;
    }

    const size_t stride = std::max<size_t>(1, nodeCount / hopSampleTarget);

    std::vector<NodeID> candidateNodes;
    std::vector<EdgeID> candidateEdges;

    size_t offered = 0;
    size_t kept = 0;

    for (size_t node = 0; node < nodeCount; node += stride) {
        const NodeID sample(node);
        const size_t owner = parts.ownerIndex(sample);
        if (owner == parts.size()) {
            continue;
        }

        const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;

        candidateNodes.clear();
        candidateEdges.clear();

        if (direction != PathExplorationDir::BACKWARD) {
            appendMatching(ownerIndexer.getNodeOutEdges(sample), edgeType, candidateNodes, candidateEdges);
        }
        if (direction != PathExplorationDir::FORWARD) {
            appendMatching(ownerIndexer.getNodeInEdges(sample), edgeType, candidateNodes, candidateEdges);
        }

        if (candidateNodes.empty()) {
            continue;
        }

        offered += candidateNodes.size();
        kept += hopFilter.filter(sample, candidateNodes, candidateEdges);
    }

    if (offered == 0) {
        return 1.0;
    }

    return static_cast<double>(kept) / static_cast<double>(offered);
}

bool PathDistanceIndex::isWorthBuilding(const GraphView& view,
                                        PathExplorationDir direction,
                                        std::optional<EdgeTypeID> edgeType,
                                        size_t seedCount,
                                        uint64_t maxHops,
                                        double hopPassRate) {
    const PartDirectory parts(view);
    const double indexCost = indexUnitCostInChecks * static_cast<double>(parts.getAllocatedNodeCount() + parts.getAllocatedEdgeCount());

    TypeBranching branching;
    sampleBranching(parts, direction, edgeType, branching);

    return estimatedEnumerationChecks(parts, branching, seedCount, maxHops, hopPassRate) > indexCost;
}

void PathDistanceIndex::collectEnds(const PartDirectory& parts, const LabelSet& endLabels, std::vector<NodeID>& ends) {
    const LabelSetHandle required(endLabels);

    for (size_t partIndex = 0; partIndex < parts.size(); partIndex++) {
        const NodeContainer& nodes = *parts.get(partIndex)._nodes;
        const LabelSetIndexer<NodeRange>& ranges = nodes.getLabelSetIndexer();

        for (auto match = ranges.matchIterate(required); match.isValid(); match.next()) {
            for (const NodeID node : match.getValue()) {
                _distances[node.getValue()] = 0;
                _reached++;
                ends.push_back(node);
            }
        }
    }
}

void PathDistanceIndex::relax(std::span<const EdgeRecord> edges,
                              uint8_t level,
                              std::optional<EdgeTypeID> edgeType,
                              const Tombstones* tombstones,
                              std::vector<NodeID>& next) {
    for (const EdgeRecord& record : edges) {
        const bool wrongType = edgeType && record._edgeTypeID != *edgeType;
        const bool deleted = tombstones && tombstones->containsEdge(record._edgeID);
        if (wrongType || deleted) {
            continue;
        }

        uint8_t& distance = _distances[record._otherID.getValue()];
        if (distance != unreachable) {
            continue;
        }

        distance = level;
        _reached++;
        next.push_back(record._otherID);
    }
}
