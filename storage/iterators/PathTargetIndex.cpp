#include "PathTargetIndex.h"

#include <algorithm>

#include "PartDirectory.h"
#include "PathDistanceIndex.h"
#include "indexers/EdgeIndexer.h"
#include "versioning/Tombstones.h"

using namespace db;

PathTargetIndex::PathTargetIndex() {
}

PathTargetIndex::~PathTargetIndex() {
}

void PathTargetIndex::build(const GraphView& view,
                            std::span<const NodeID> targets,
                            PathExplorationDir direction,
                            std::optional<EdgeTypeID> edgeType,
                            uint64_t maxHops) {
    const PartDirectory parts(view);
    const Tombstones& tombstones = view.tombstones();
    const Tombstones* edgeTombstones = tombstones.hasEdges() ? &tombstones : nullptr;
    const uint64_t levelCap = std::min<uint64_t>(maxHops, PathDistanceIndex::farthest);

    _handles.clear();
    _batches.clear();

    // The handles point into the batches, so the vector must not grow under them
    const size_t batchCount = (targets.size() + targetsPerBatch - 1) / targetsPerBatch;
    _batches.reserve(batchCount);

    for (size_t first = 0; first < targets.size(); first += targetsPerBatch) {
        const size_t count = std::min(targetsPerBatch, targets.size() - first);
        PathTargetHandle::Levels& levels = _batches.emplace_back();

        buildBatch(parts, targets.subspan(first, count), direction, edgeType, edgeTombstones, levelCap, levels);
    }

    _built = true;
}

PathTargetHandle PathTargetIndex::find(NodeID target) const {
    const auto handleIt = _handles.find(target.getValue());
    if (handleIt == _handles.end()) {
        return PathTargetHandle {};
    }

    return handleIt->second;
}

bool PathTargetIndex::isWorthBuilding(const GraphView& view,
                                      PathExplorationDir direction,
                                      size_t seedCount,
                                      size_t targetCount,
                                      uint64_t maxHops) {
    if (targetCount == 0) {
        return false;
    }

    // Each batch is one pass of its own, so the enumeration has to pay for all of them
    const size_t batchCount = (targetCount + targetsPerBatch - 1) / targetsPerBatch;

    return PathDistanceIndex::isWorthBuilding(view, direction, seedCount / batchCount, maxHops);
}

void PathTargetIndex::buildBatch(const PartDirectory& parts,
                                 std::span<const NodeID> targets,
                                 PathExplorationDir direction,
                                 std::optional<EdgeTypeID> edgeType,
                                 const Tombstones* tombstones,
                                 uint64_t levelCap,
                                 PathTargetHandle::Levels& levels) {
    const size_t nodeCount = parts.getAllocatedNodeCount();
    levels.emplace_back(nodeCount, 0);

    std::vector<NodeID> frontier;
    for (size_t bit = 0; bit < targets.size(); bit++) {
        const NodeID target = targets[bit];
        if (target.getValue() >= nodeCount) {
            continue;
        }

        const uint64_t mask = 1ull << bit;
        levels[0][target.getValue()] |= mask;
        frontier.push_back(target);
        _handles[target.getValue()] = PathTargetHandle {&levels, mask};
    }

    const bool walksIns = direction != PathExplorationDir::BACKWARD;
    const bool walksOuts = direction != PathExplorationDir::FORWARD;

    std::vector<uint8_t> queued(nodeCount, 0);
    std::vector<NodeID> next;

    for (uint64_t level = 1; level <= levelCap && !frontier.empty(); level++) {
        // Each level's words are the previous level's plus what one more hop reaches
        levels.push_back(levels.back());
        std::vector<uint64_t>& reached = levels.back();
        const std::vector<uint64_t>& previous = levels[level - 1];

        next.clear();
        for (const NodeID node : frontier) {
            const uint64_t word = previous[node.getValue()];
            const size_t owner = parts.ownerIndex(node);
            if (owner == parts.size()) {
                continue;
            }

            const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;
            if (walksIns) {
                relax(ownerIndexer.getNodeInEdges(node), word, edgeType, tombstones, reached, queued, next);
            }
            if (walksOuts) {
                relax(ownerIndexer.getNodeOutEdges(node), word, edgeType, tombstones, reached, queued, next);
            }

            for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
                const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
                if (walksIns) {
                    relax(patchIndexer.getNodeInEdges(node), word, edgeType, tombstones, reached, queued, next);
                }
                if (walksOuts) {
                    relax(patchIndexer.getNodeOutEdges(node), word, edgeType, tombstones, reached, queued, next);
                }
            }
        }

        for (const NodeID node : next) {
            queued[node.getValue()] = 0;
        }

        std::swap(frontier, next);
    }
}

void PathTargetIndex::relax(std::span<const EdgeRecord> edges,
                            uint64_t word,
                            std::optional<EdgeTypeID> edgeType,
                            const Tombstones* tombstones,
                            std::vector<uint64_t>& reached,
                            std::vector<uint8_t>& queued,
                            std::vector<NodeID>& next) {
    for (const EdgeRecord& record : edges) {
        const bool wrongType = edgeType && record._edgeTypeID != *edgeType;
        const bool deleted = tombstones && tombstones->containsEdge(record._edgeID);
        if (wrongType || deleted) {
            continue;
        }

        const size_t other = record._otherID.getValue();
        uint64_t& reachedWord = reached[other];
        const uint64_t gained = word & ~reachedWord;
        if (gained == 0) {
            continue;
        }

        reachedWord |= gained;
        if (!queued[other]) {
            queued[other] = 1;
            next.push_back(record._otherID);
        }
    }
}
