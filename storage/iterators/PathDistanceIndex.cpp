#include "PathDistanceIndex.h"

#include <algorithm>
#include <limits>
#include <math.h>

#include "EdgeTypeMatch.h"
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
// it lower still, a wider base spending the budget earlier.
constexpr size_t seedSampleTarget = 64;
constexpr size_t seedSampleLevels = 6;
constexpr size_t seedSampleBudget = 4096;

// The ends whose adjacency prices the first level of a budgeted search
constexpr size_t endSampleTarget = 256;

// A prime past any frontier the budget allows: stepping by it modulo the frontier's size
// visits every node once, in an order unrelated to the one they were reached in
constexpr size_t scatterStride = 2654435761;

// Fewer than the fan-out sample takes: every node of this one costs an evaluation of the
// query's hop predicate rather than a count of its adjacency
constexpr size_t hopSampleTarget = 1024;

void appendMatching(std::span<const EdgeRecord> edges,
                    std::span<const EdgeTypeID> edgeTypes,
                    std::vector<NodeID>& candidateNodes,
                    std::vector<EdgeID>& candidateEdges) {
    for (const EdgeRecord& record : edges) {
        if (!edgeTypes.empty() && !edgeTypeMatches(edgeTypes, record._edgeTypeID)) {
            continue;
        }

        candidateNodes.push_back(record._otherID);
        candidateEdges.push_back(record._edgeID);
    }
}

size_t appendMatchingNodes(std::span<const EdgeRecord> edges,
                           std::span<const EdgeTypeID> edgeTypes,
                           std::vector<NodeID>& nodes) {
    size_t appended = 0;
    for (const EdgeRecord& record : edges) {
        if (!edgeTypes.empty() && !edgeTypeMatches(edgeTypes, record._edgeTypeID)) {
            continue;
        }

        nodes.push_back(record._otherID);
        appended++;
    }

    return appended;
}

double nodeTouch(const PartDirectory& parts, NodeID node, bool walksIns, bool walksOuts) {
    const size_t owner = parts.ownerIndex(node);
    if (owner == parts.size()) {
        return 1.0;
    }

    double touched = 1.0;
    const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;
    if (walksIns) {
        touched += static_cast<double>(ownerIndexer.getNodeInEdges(node).size());
    }
    if (walksOuts) {
        touched += static_cast<double>(ownerIndexer.getNodeOutEdges(node).size());
    }

    for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
        const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
        if (walksIns) {
            touched += static_cast<double>(patchIndexer.getNodeInEdges(node).size());
        }
        if (walksOuts) {
            touched += static_cast<double>(patchIndexer.getNodeOutEdges(node).size());
        }
    }

    return touched;
}

double sampledFirstLevelTouch(const PartDirectory& parts, const LabelSet& endLabels, bool walksIns, bool walksOuts) {
    const LabelSetHandle required(endLabels);

    size_t endCount = 0;
    for (size_t partIndex = 0; partIndex < parts.size(); partIndex++) {
        const LabelSetIndexer<NodeRange>& ranges = parts.get(partIndex)._nodes->getLabelSetIndexer();
        for (auto match = ranges.matchIterate(required); match.isValid(); match.next()) {
            endCount += match.getValue()._count;
        }
    }

    if (endCount == 0) {
        return 0.0;
    }

    const size_t stride = std::max<size_t>(1, endCount / endSampleTarget);
    size_t position = 0;
    size_t sampled = 0;
    double touched = 0.0;

    for (size_t partIndex = 0; partIndex < parts.size(); partIndex++) {
        const LabelSetIndexer<NodeRange>& ranges = parts.get(partIndex)._nodes->getLabelSetIndexer();
        for (auto match = ranges.matchIterate(required); match.isValid(); match.next()) {
            const NodeRange& range = match.getValue();
            for (size_t offset = (stride - position % stride) % stride; offset < range._count; offset += stride) {
                touched += nodeTouch(parts, range._first + offset, walksIns, walksOuts);
                sampled++;
            }
            position += range._count;
        }
    }

    return touched / static_cast<double>(sampled) * static_cast<double>(endCount);
}

void gatherEnds(const PartDirectory& parts, const LabelSet& endLabels, std::vector<NodeID>& ends) {
    const LabelSetHandle required(endLabels);

    for (size_t partIndex = 0; partIndex < parts.size(); partIndex++) {
        const NodeContainer& nodes = *parts.get(partIndex)._nodes;
        const LabelSetIndexer<NodeRange>& ranges = nodes.getLabelSetIndexer();

        for (auto match = ranges.matchIterate(required); match.isValid(); match.next()) {
            for (const NodeID node : match.getValue()) {
                ends.push_back(node);
            }
        }
    }
}

double levelTouch(const PartDirectory& parts, std::span<const NodeID> frontier, bool walksIns, bool walksOuts) {
    double touched = 0.0;
    for (const NodeID node : frontier) {
        touched += nodeTouch(parts, node, walksIns, walksOuts);
    }

    return touched;
}

size_t countMatching(std::span<const EdgeRecord> edges, std::span<const EdgeTypeID> edgeTypes) {
    if (edgeTypes.empty()) {
        return edges.size();
    }

    size_t matching = 0;
    for (const EdgeRecord& record : edges) {
        if (edgeTypeMatches(edgeTypes, record._edgeTypeID)) {
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

void PathDistanceIndex::clear() {
    _distances.clear();
    _reached = 0;
    _built = false;
    _overrunBudget = 0.0;
    _leastBuildChecks = 0.0;
}

void PathDistanceIndex::build(const GraphView& view,
                              const LabelSet& endLabels,
                              PathExplorationDir direction,
                              std::span<const EdgeTypeID> edgeTypes,
                              uint64_t maxHops) {
    const PartDirectory parts(view);

    _distances.assign(parts.getAllocatedNodeCount(), unreachable);
    _reached = 0;

    std::vector<NodeID> frontier;
    gatherEnds(parts, endLabels, frontier);
    markEnds(frontier);

    search(parts, view.tombstones(), frontier, direction, edgeTypes, maxHops, std::numeric_limits<double>::infinity());
    _built = true;
}

bool PathDistanceIndex::buildWithin(const GraphView& view,
                                    const LabelSet& endLabels,
                                    PathExplorationDir direction,
                                    std::span<const EdgeTypeID> edgeTypes,
                                    uint64_t maxHops,
                                    double budgetChecks) {
    const bool overranBefore = _overrunBudget > 0.0;
    if (overranBefore && budgetChecks < 2.0 * _overrunBudget) {
        return false;
    } else if (budgetChecks < _leastBuildChecks) {
        return false;
    }

    const PartDirectory parts(view);
    const bool walksIns = direction != PathExplorationDir::BACKWARD;
    const bool walksOuts = direction != PathExplorationDir::FORWARD;

    // The first level is paid whatever lies beyond it: a budget short of it is refused before
    // the ends are gathered, and every later one short of it without pricing it again
    const double filledChecks = filledByteCostInChecks * static_cast<double>(parts.getAllocatedNodeCount());
    const double firstLevelTouch = sampledFirstLevelTouch(parts, endLabels, walksIns, walksOuts);
    _leastBuildChecks = filledChecks + indexUnitCostInChecks * firstLevelTouch;
    if (budgetChecks < _leastBuildChecks) {
        return false;
    }

    std::vector<NodeID> frontier;
    gatherEnds(parts, endLabels, frontier);

    _distances.assign(parts.getAllocatedNodeCount(), unreachable);
    _reached = 0;
    markEnds(frontier);

    // No search touches more than every node and, per direction it walks, every edge
    const double directionCount = (walksIns ? 1.0 : 0.0) + (walksOuts ? 1.0 : 0.0);
    const double graphTouch = static_cast<double>(parts.getAllocatedNodeCount())
                            + directionCount * static_cast<double>(parts.getAllocatedEdgeCount());
    const double touchBudget = (budgetChecks - filledChecks) / indexUnitCostInChecks;
    const double searchBudget = touchBudget >= graphTouch ? std::numeric_limits<double>::infinity() : touchBudget;
    const bool finished = search(parts, view.tombstones(), frontier, direction, edgeTypes, maxHops, searchBudget);

    if (!finished) {
        _distances.clear();
        _reached = 0;
        _overrunBudget = budgetChecks;
        return false;
    }

    _built = true;

    return true;
}

void PathDistanceIndex::build(const GraphView& view,
                              std::span<const NodeID> ends,
                              PathExplorationDir direction,
                              std::span<const EdgeTypeID> edgeTypes,
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

    search(parts, view.tombstones(), frontier, direction, edgeTypes, maxHops, std::numeric_limits<double>::infinity());
    _built = true;
}

bool PathDistanceIndex::search(const PartDirectory& parts,
                               const Tombstones& tombstones,
                               std::vector<NodeID>& frontier,
                               PathExplorationDir direction,
                               std::span<const EdgeTypeID> edgeTypes,
                               uint64_t maxHops,
                               double touchBudget) {
    // A hop the exploration takes forward is walked back here: the distances of the nodes
    // an out-edge leaves grow along in-edges
    const bool walksIns = direction != PathExplorationDir::BACKWARD;
    const bool walksOuts = direction != PathExplorationDir::FORWARD;

    const Tombstones* edgeTombstones = tombstones.hasEdges() ? &tombstones : nullptr;

    std::vector<NodeID> next;

    // A level's cost is its frontier and the edges it reads, known before one is read: a
    // level that cannot finish within the budget is never started
    const bool budgeted = touchBudget < std::numeric_limits<double>::infinity();
    double touched = 0.0;

    // A distance saturates at the farthest a byte can hold, so past that one reads "at least
    // that many hops". canReachEndWithin then keeps a node a deeper bound may not be able to
    // use, which costs a walk that never completes; capping the search instead would drop it.
    for (uint64_t level = 1; level <= maxHops && !frontier.empty(); level++) {
        const uint8_t distance = static_cast<uint8_t>(std::min<uint64_t>(level, farthest));
        next.clear();

        if (budgeted) {
            touched += levelTouch(parts, frontier, walksIns, walksOuts);
            if (touched > touchBudget) {
                return false;
            }
        }

        for (const NodeID node : frontier) {
            const size_t owner = parts.ownerIndex(node);
            if (owner == parts.size()) {
                continue;
            }

            const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;
            if (walksIns) {
                relax(ownerIndexer.getNodeInEdges(node), distance, edgeTypes, edgeTombstones, next);
            }
            if (walksOuts) {
                relax(ownerIndexer.getNodeOutEdges(node), distance, edgeTypes, edgeTombstones, next);
            }

            for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
                const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
                if (walksIns) {
                    relax(patchIndexer.getNodeInEdges(node), distance, edgeTypes, edgeTombstones, next);
                }
                if (walksOuts) {
                    relax(patchIndexer.getNodeOutEdges(node), distance, edgeTypes, edgeTombstones, next);
                }
            }
        }

        std::swap(frontier, next);
    }

    return true;
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
                                        std::span<const EdgeTypeID> edgeTypes,
                                        TypeBranching& branching) {
    branching = TypeBranching {};

    const size_t nodeCount = parts.getAllocatedNodeCount();
    if (nodeCount == 0) {
        return;
    }

    const size_t edgeCount = parts.getAllocatedEdgeCount();
    EdgeBranchingCache& cache = parts.getBranchingCache();
    if (cache.lookup(direction, edgeTypes, nodeCount, edgeCount, branching)) {
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

        const size_t outs = countMatching(ownerIndexer.getNodeOutEdges(sample), edgeTypes);
        const size_t ins = countMatching(ownerIndexer.getNodeInEdges(sample), edgeTypes);

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

    cache.store(direction, edgeTypes, nodeCount, edgeCount, branching);
}

void PathDistanceIndex::sampleSeedExpansion(const PartDirectory& parts,
                                            PathExplorationDir direction,
                                            std::span<const EdgeTypeID> edgeTypes,
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

        // A level lists its nodes parent by parent, so the part the budget lets through has to
        // be taken across the whole frontier, not from its front: its first parents need not
        // branch like the rest
        const size_t frontierSize = frontier.size();
        for (size_t visit = 0; visit < frontierSize; visit++) {
            if (next.size() >= seedSampleBudget) {
                break;
            }

            const NodeID node = frontier[(visit * scatterStride) % frontierSize];
            const size_t owner = parts.ownerIndex(node);
            if (owner == parts.size()) {
                continue;
            }

            const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;

            size_t continuing = 0;
            if (walksOuts) {
                continuing += appendMatchingNodes(ownerIndexer.getNodeOutEdges(node), edgeTypes, next);
            }
            if (walksIns) {
                continuing += appendMatchingNodes(ownerIndexer.getNodeInEdges(node), edgeTypes, next);
            }

            for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
                const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
                if (walksOuts) {
                    continuing += appendMatchingNodes(patchIndexer.getNodeOutEdges(node), edgeTypes, next);
                }
                if (walksIns) {
                    continuing += appendMatchingNodes(patchIndexer.getNodeInEdges(node), edgeTypes, next);
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
                                                std::span<const EdgeTypeID> edgeTypes,
                                                size_t sourceCount,
                                                uint64_t maxHops) {
    const size_t nodeCount = parts.getAllocatedNodeCount();
    const size_t edgeCount = parts.getAllocatedEdgeCount();
    if (sourceCount == 0 || maxHops == 0 || nodeCount == 0 || edgeCount == 0) {
        return 0.0;
    }

    // The search walks the ends back against @param direction, so it is the reverse walk's
    // branching that prices it: the support of one direction is the graph's other end
    TypeBranching branching;
    sampleBranching(parts, reverseOf(direction), edgeTypes, branching);

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
                                            std::span<const EdgeTypeID> edgeTypes,
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
            appendMatching(ownerIndexer.getNodeOutEdges(sample), edgeTypes, candidateNodes, candidateEdges);
        }
        if (direction != PathExplorationDir::FORWARD) {
            appendMatching(ownerIndexer.getNodeInEdges(sample), edgeTypes, candidateNodes, candidateEdges);
        }

        if (candidateNodes.empty()) {
            continue;
        }

        offered += candidateNodes.size();
        kept += hopFilter.filter(0, sample, candidateNodes, candidateEdges);
    }

    if (offered == 0) {
        return 1.0;
    }

    return static_cast<double>(kept) / static_cast<double>(offered);
}

double PathDistanceIndex::estimatedBuildChecks(const PartDirectory& parts,
                                               PathExplorationDir direction,
                                               std::span<const EdgeTypeID> edgeTypes,
                                               size_t sourceCount,
                                               uint64_t maxHops) {
    const double nodeCount = static_cast<double>(parts.getAllocatedNodeCount());
    const double graph = nodeCount + static_cast<double>(parts.getAllocatedEdgeCount());
    const double touched = std::min(graph, estimatedSearchChecks(parts, direction, edgeTypes, sourceCount, maxHops));

    return indexUnitCostInChecks * touched + filledByteCostInChecks * nodeCount;
}

void PathDistanceIndex::markEnds(std::span<const NodeID> ends) {
    for (const NodeID end : ends) {
        _distances[end.getValue()] = 0;
        _reached++;
    }
}

void PathDistanceIndex::relax(std::span<const EdgeRecord> edges,
                              uint8_t level,
                              std::span<const EdgeTypeID> edgeTypes,
                              const Tombstones* tombstones,
                              std::vector<NodeID>& next) {
    for (const EdgeRecord& record : edges) {
        const bool wrongType = !edgeTypes.empty() && !edgeTypeMatches(edgeTypes, record._edgeTypeID);
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
