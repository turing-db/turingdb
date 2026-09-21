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

// Filling the distance byte of every node before the search: 0.02 ns a byte measured
// against 47 ns a check on reactome
constexpr double filledByteCostInChecks = 0.0005;

constexpr size_t fanOutSampleTarget = 4096;

// The seeds expanded to measure what a walk costs, the levels they are expanded over and the
// nodes one level of that expansion may reach. Measured on reactome, against a walk of every
// reaction whose own cost is known: three levels read it five times under, since most seeds
// die at once and the levels past them are where the survivors show, and six read it within
// a tenth. 16 seeds read a broad set half of what 64 does, and 256 cost four times 64 to read
// it lower still, a wider base spending the budget earlier. The budget only binds on an
// untyped walk, where quadrupling it costs 2.5x and moves the reading by 1%.
constexpr size_t seedSampleTarget = 64;
constexpr size_t seedSampleLevels = 6;
constexpr size_t seedSampleBudget = 4096;

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

size_t appendMatchingNodes(std::span<const EdgeRecord> edges,
                           std::optional<EdgeTypeID> edgeType,
                           std::vector<NodeID>& nodes) {
    size_t appended = 0;
    for (const EdgeRecord& record : edges) {
        if (edgeType && record._edgeTypeID != *edgeType) {
            continue;
        }

        nodes.push_back(record._otherID);
        appended++;
    }

    return appended;
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

    search(parts, view.tombstones(), frontier, direction, edgeType, maxHops);
    _built = true;
}

void PathDistanceIndex::build(const GraphView& view,
                              std::span<const NodeID> ends,
                              PathExplorationDir direction,
                              std::optional<EdgeTypeID> edgeType,
                              uint64_t maxHops) {
    const PartDirectory parts(view);

    _distances.assign(parts.getAllocatedNodeCount(), unreachable);
    _reached = 0;

    std::vector<NodeID> frontier;
    for (const NodeID end : ends) {
        const size_t index = end.getValue();
        if (index >= _distances.size() || _distances[index] != unreachable) {
            continue;
        }

        _distances[index] = 0;
        _reached++;
        frontier.push_back(end);
    }

    search(parts, view.tombstones(), frontier, direction, edgeType, maxHops);
    _built = true;
}

void PathDistanceIndex::search(const PartDirectory& parts,
                               const Tombstones& tombstones,
                               std::vector<NodeID>& frontier,
                               PathExplorationDir direction,
                               std::optional<EdgeTypeID> edgeType,
                               uint64_t maxHops) {
    // A hop the exploration takes forward is walked back here: the distances of the nodes
    // an out-edge leaves grow along in-edges
    const bool walksIns = direction != PathExplorationDir::BACKWARD;
    const bool walksOuts = direction != PathExplorationDir::FORWARD;

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

void PathDistanceIndex::sampleSeedExpansion(const PartDirectory& parts,
                                            PathExplorationDir direction,
                                            std::optional<EdgeTypeID> edgeType,
                                            std::span<const NodeID> seeds,
                                            SeedExpansion& expansion) {
    expansion = SeedExpansion {};

    if (seeds.empty() || parts.getAllocatedNodeCount() == 0) {
        return;
    }

    const bool walksOuts = direction != PathExplorationDir::BACKWARD;
    const bool walksIns = direction != PathExplorationDir::FORWARD;

    const size_t stride = std::max<size_t>(1, seeds.size() / seedSampleTarget);

    std::vector<NodeID> frontier;
    for (size_t seed = 0; seed < seeds.size(); seed += stride) {
        frontier.push_back(seeds[seed]);
    }

    double frontierPerSeed = 1.0;

    std::vector<NodeID> next;
    for (size_t level = 0; level < seedSampleLevels && !frontier.empty(); level++) {
        next.clear();

        double arrivals = 0.0;
        double continuations = 0.0;

        for (const NodeID node : frontier) {
            if (next.size() >= seedSampleBudget) {
                break;
            }

            const size_t owner = parts.ownerIndex(node);
            if (owner == parts.size()) {
                continue;
            }

            const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;

            size_t continuing = 0;
            if (walksOuts) {
                continuing += appendMatchingNodes(ownerIndexer.getNodeOutEdges(node), edgeType, next);
            }
            if (walksIns) {
                continuing += appendMatchingNodes(ownerIndexer.getNodeInEdges(node), edgeType, next);
            }

            for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
                const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
                if (walksOuts) {
                    continuing += appendMatchingNodes(patchIndexer.getNodeOutEdges(node), edgeType, next);
                }
                if (walksIns) {
                    continuing += appendMatchingNodes(patchIndexer.getNodeInEdges(node), edgeType, next);
                }
            }

            arrivals += 1.0;
            continuations += static_cast<double>(continuing);
        }

        if (arrivals == 0.0) {
            break;
        }

        // A level's ratio is measured over the nodes of it the budget let through, so a
        // truncated level still reports what the frontier it sampled branched by
        const double levelFanOut = continuations / arrivals;

        frontierPerSeed = frontierPerSeed * levelFanOut;
        expansion._frontierPerSeed[expansion._levels] = frontierPerSeed;
        expansion._levels++;
        expansion._tailFanOut = levelFanOut;

        std::swap(frontier, next);
    }
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
                                                     const SeedExpansion& expansion,
                                                     size_t seedCount,
                                                     uint64_t maxHops,
                                                     double hopPassRate) {
    const size_t nodeCount = parts.getAllocatedNodeCount();
    const size_t edgeCount = parts.getAllocatedEdgeCount();
    if (seedCount == 0 || maxHops == 0 || nodeCount == 0 || edgeCount == 0) {
        return 0.0;
    }

    const uint64_t levelCount = std::min<uint64_t>(maxHops, farthest);
    const uint64_t measured = std::min<uint64_t>(expansion._levels, levelCount);
    const double tailFanOut = std::max(1.0, expansion._tailFanOut);

    double candidatesPerSeed = 0.0;
    double frontier = 1.0;
    double pass = 1.0;

    for (uint64_t level = 0; level < measured; level++) {
        const double checks = expansion._frontierPerSeed[level] * pass;

        candidatesPerSeed += checks;
        frontier = checks * hopPassRate;
        pass *= hopPassRate;
    }

    for (uint64_t level = measured; level < levelCount; level++) {
        const double checks = frontier * tailFanOut;

        candidatesPerSeed += checks;
        frontier = checks * hopPassRate;
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

double PathDistanceIndex::estimatedBuildChecks(const PartDirectory& parts,
                                               PathExplorationDir direction,
                                               std::optional<EdgeTypeID> edgeType,
                                               size_t sourceCount,
                                               uint64_t maxHops) {
    const double nodeCount = static_cast<double>(parts.getAllocatedNodeCount());
    const double graph = nodeCount + static_cast<double>(parts.getAllocatedEdgeCount());
    const double touched = std::min(graph, estimatedSearchChecks(parts, direction, edgeType, sourceCount, maxHops));

    return indexUnitCostInChecks * touched + filledByteCostInChecks * nodeCount;
}

bool PathDistanceIndex::isWorthBuilding(const GraphView& view,
                                        const SeedExpansion& expansion,
                                        size_t seedCount,
                                        uint64_t maxHops,
                                        double hopPassRate) {
    const PartDirectory parts(view);
    const double indexCost = indexUnitCostInChecks * static_cast<double>(parts.getAllocatedNodeCount() + parts.getAllocatedEdgeCount());

    return estimatedEnumerationChecks(parts, expansion, seedCount, maxHops, hopPassRate) > indexCost;
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
