#include "PathDistanceIndex.h"

#include <algorithm>
#include <math.h>

#include "PartDirectory.h"
#include "datapart/NodeContainer.h"
#include "datapart/NodeRange.h"
#include "indexers/EdgeIndexer.h"
#include "indexers/LabelSetIndexer.h"
#include "metadata/LabelSetHandle.h"
#include "versioning/Tombstones.h"

using namespace db;

namespace {

// The enumeration is estimated as the seeds fanning out over this many hops at most: past
// it the estimate is a guess about the graph, not about the query
constexpr uint64_t estimatedDepthCap = 4;

// The index costs one pass over the nodes and edges it reaches, at most the whole graph;
// it is built when the enumeration is expected to cost this many times more
constexpr double indexCostMultiple = 8.0;

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
    const uint64_t levelCap = std::min<uint64_t>(maxHops, farthest);

    std::vector<NodeID> next;
    for (uint64_t level = 1; level <= levelCap && !frontier.empty(); level++) {
        const uint8_t distance = static_cast<uint8_t>(level);
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

bool PathDistanceIndex::isWorthBuilding(const GraphView& view,
                                        PathExplorationDir direction,
                                        size_t seedCount,
                                        uint64_t maxHops) {
    if (seedCount == 0 || maxHops == 0) {
        return false;
    }

    const PartDirectory parts(view);
    const size_t nodeCount = parts.getAllocatedNodeCount();
    const size_t edgeCount = parts.getAllocatedEdgeCount();
    if (nodeCount == 0 || edgeCount == 0) {
        return false;
    }

    const double directions = direction == PathExplorationDir::BOTH ? 2.0 : 1.0;
    const double fanOut = std::max(1.0, directions * static_cast<double>(edgeCount) / static_cast<double>(nodeCount));
    const double depth = static_cast<double>(std::min(maxHops, estimatedDepthCap));
    const double enumerationCost = static_cast<double>(seedCount) * pow(fanOut, depth);
    const double indexCost = indexCostMultiple * static_cast<double>(nodeCount + edgeCount);

    return enumerationCost > indexCost;
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
