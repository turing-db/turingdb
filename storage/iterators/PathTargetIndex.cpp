#include "PathTargetIndex.h"

#include <algorithm>
#include <bit>

#include "EdgeTypeMatch.h"
#include "PartDirectory.h"
#include "PathDistanceIndex.h"
#include "indexers/EdgeIndexer.h"
#include "versioning/Tombstones.h"

using namespace db;

namespace {

// What the two layouts cost in candidate checks of the walk they spare, measured with
// samples/path_bench: a sparse batch pays one table probe and insertion per node it reaches,
// between 3.5 and 7 checks; a dense batch writes one word per node per level sequentially,
// about a tenth of a check each. A sparse slot is this many bytes, kept at most half full, and
// the tables or words of one build stay under this many bytes whatever the walk would cost.
// One node the search reaches costs this many candidate checks of the walk it spares:
// measured at 78 ns a node against 30 ns a check on a generated degree-8 graph, and 141
// against 47 on reactome, so the higher of the two shapes with nothing rounded away
constexpr double reachedNodeCostInChecks = 3.0;
constexpr double wordCostInChecks = 0.1;
constexpr double bytesPerReachedNode = 2.0 * (sizeof(uint64_t) + sizeof(uint64_t) + PathTargetBatch::targetsPerBatch + sizeof(uint8_t));
constexpr double bytesLimit = 1024.0 * 1024.0 * 1024.0;

}

PathTargetBatch::PathTargetBatch() {
    allocate(initialCapacity);
}

PathTargetBatch::~PathTargetBatch() {
}

void PathTargetBatch::setDense(size_t nodeCount) {
    _dense = true;
    _levels.assign(1, std::vector<uint64_t>(nodeCount, 0));
    _denseQueued.assign(nodeCount, 0);

    _keys.clear();
    _reached.clear();
    _distances.clear();
    _queued.clear();
}

uint64_t PathTargetBatch::getReached(NodeID node) const {
    if (_dense) {
        const size_t index = node.getValue();
        const std::vector<uint64_t>& reached = _levels.back();

        return index < reached.size() ? reached[index] : 0;
    }

    const size_t slot = find(node);

    return slot == _keys.size() ? 0 : _reached[slot];
}

uint64_t PathTargetBatch::gain(NodeID node, uint64_t word, uint8_t distance) {
    if (_dense) {
        const size_t index = node.getValue();
        std::vector<uint64_t>& reached = _levels.back();
        if (index >= reached.size()) {
            return 0;
        }

        const uint64_t gained = word & ~reached[index];
        if (gained != 0 && reached[index] == 0) {
            _count++;
        }
        reached[index] |= gained;

        return gained;
    }

    const size_t slot = findOrInsert(node);
    const uint64_t gained = word & ~_reached[slot];

    // The search opens the levels in order, so the first targets a node gains are its nearest
    if (_reached[slot] == 0 && gained != 0) {
        _minDistances[slot] = distance;
    }

    _reached[slot] |= gained;

    uint8_t* distances = &_distances[slot * targetsPerBatch];
    for (uint64_t bits = gained; bits != 0; bits &= bits - 1) {
        distances[std::countr_zero(bits)] = distance;
    }

    return gained;
}

bool PathTargetBatch::isQueued(NodeID node) const {
    if (_dense) {
        return _denseQueued[node.getValue()] != 0;
    }

    return _queued[find(node)] != 0;
}

void PathTargetBatch::setQueued(NodeID node, bool queued) {
    if (_dense) {
        _denseQueued[node.getValue()] = queued ? 1 : 0;
        return;
    }

    _queued[find(node)] = queued ? 1 : 0;
}

void PathTargetBatch::beginLevel() {
    if (_dense && _levels.size() <= farthestLevel) {
        _levels.push_back(_levels.back());
    }
}

void PathTargetBatch::allocate(size_t capacity) {
    _keys.assign(capacity, emptyKey);
    _reached.assign(capacity, 0);
    _distances.assign(capacity * targetsPerBatch, unreached);
    _minDistances.assign(capacity, unreached);
    _queued.assign(capacity, 0);
    _mask = capacity - 1;
    _count = 0;
}

size_t PathTargetBatch::findOrInsert(NodeID node) {
    if ((_count + 1) * 2 > _keys.size()) {
        grow();
    }

    const uint64_t key = node.getValue();
    size_t slot = hashOf(key) & _mask;

    while (_keys[slot] != emptyKey) {
        if (_keys[slot] == key) {
            return slot;
        }

        slot = (slot + 1) & _mask;
    }

    _keys[slot] = key;
    _count++;

    return slot;
}

void PathTargetBatch::grow() {
    const std::vector<uint64_t> keys = std::move(_keys);
    const std::vector<uint64_t> reached = std::move(_reached);
    const std::vector<uint8_t> distances = std::move(_distances);
    const std::vector<uint8_t> minDistances = std::move(_minDistances);
    const std::vector<uint8_t> queued = std::move(_queued);

    allocate(keys.size() * 2);

    for (size_t oldSlot = 0; oldSlot < keys.size(); oldSlot++) {
        if (keys[oldSlot] == emptyKey) {
            continue;
        }

        size_t slot = hashOf(keys[oldSlot]) & _mask;
        while (_keys[slot] != emptyKey) {
            slot = (slot + 1) & _mask;
        }

        _keys[slot] = keys[oldSlot];
        _reached[slot] = reached[oldSlot];
        _minDistances[slot] = minDistances[oldSlot];
        _queued[slot] = queued[oldSlot];
        std::copy_n(&distances[oldSlot * targetsPerBatch], targetsPerBatch, &_distances[slot * targetsPerBatch]);
        _count++;
    }
}

PathTargetIndex::PathTargetIndex() {
}

PathTargetIndex::~PathTargetIndex() {
}

void PathTargetIndex::planBatch(const PartDirectory& parts,
                                PathExplorationDir direction,
                                std::span<const EdgeTypeID> edgeTypes,
                                size_t targetCount,
                                uint64_t maxHops,
                                BatchPlan& plan) {
    const double nodeCount = static_cast<double>(parts.getAllocatedNodeCount());

    // A batch's search reaches at most what its targets fan out to, and at most the graph
    const double candidatesPerTarget = PathDistanceIndex::estimatedSearchChecks(parts, direction, edgeTypes, 1, maxHops);
    const double reachedPerBatch = std::min(nodeCount, static_cast<double>(targetCount) * candidatesPerTarget);
    const double levelCount = static_cast<double>(std::min<uint64_t>(maxHops, PathDistanceIndex::farthest) + 1);
    const double words = levelCount * nodeCount;

    const double sparseChecks = reachedNodeCostInChecks * reachedPerBatch;
    const double denseChecks = wordCostInChecks * words;

    plan._dense = denseChecks < sparseChecks;
    plan._checks = plan._dense ? denseChecks : sparseChecks;
    plan._bytes = plan._dense ? words * static_cast<double>(sizeof(uint64_t)) + nodeCount : reachedPerBatch * bytesPerReachedNode;
}

void PathTargetIndex::build(const GraphView& view,
                            std::span<const NodeID> targets,
                            PathExplorationDir direction,
                            std::span<const EdgeTypeID> edgeTypes,
                            uint64_t maxHops) {
    const PartDirectory parts(view);
    const Tombstones& tombstones = view.tombstones();
    const Tombstones* edgeTombstones = tombstones.hasEdges() ? &tombstones : nullptr;

    _handles.clear();
    _batches.clear();

    // The handles point into the batches, so the vector must not grow under them
    const size_t batchCount = (targets.size() + targetsPerBatch - 1) / targetsPerBatch;
    _batches.reserve(batchCount);

    for (size_t first = 0; first < targets.size(); first += targetsPerBatch) {
        const size_t count = std::min(targetsPerBatch, targets.size() - first);

        BatchPlan plan;
        planBatch(parts, direction, edgeTypes, count, maxHops, plan);

        PathTargetBatch& batch = _batches.emplace_back();
        if (plan._dense) {
            batch.setDense(parts.getAllocatedNodeCount());
        }

        buildBatch(parts, targets.subspan(first, count), direction, edgeTypes, edgeTombstones, maxHops, batch);
    }

    _built = true;
}

// The two layouts of a set cost the same search; the smaller one is the faster to probe at
// every candidate the walk checks, so the table wins while its slots weigh less than a byte
// for every node of the graph
void PathTargetIndex::planSet(const PartDirectory& parts,
                              PathExplorationDir direction,
                              std::span<const EdgeTypeID> edgeTypes,
                              size_t targetCount,
                              uint64_t maxHops,
                              SetPlan& plan) {
    const double nodeCount = static_cast<double>(parts.getAllocatedNodeCount());
    const double candidatesPerTarget = PathDistanceIndex::estimatedSearchChecks(parts, direction, edgeTypes, 1, maxHops);
    const double reached = std::min(nodeCount, static_cast<double>(targetCount) * candidatesPerTarget);
    const double tableBytes = reached * bytesPerReachedNode;

    plan._sparse = tableBytes < nodeCount;
    plan._checks = plan._sparse ? reachedNodeCostInChecks * reached : PathDistanceIndex::estimatedBuildChecks(parts, direction, edgeTypes, targetCount, maxHops);
    plan._bytes = plan._sparse ? tableBytes : nodeCount;
}

void PathTargetIndex::buildSet(const GraphView& view,
                               std::span<const NodeID> targets,
                               PathExplorationDir direction,
                               std::span<const EdgeTypeID> edgeTypes,
                               uint64_t maxHops) {
    const PartDirectory parts(view);

    SetPlan plan;
    planSet(parts, direction, edgeTypes, targets.size(), maxHops, plan);

    _handles.clear();
    _batches.clear();

    if (plan._sparse) {
        const Tombstones& tombstones = view.tombstones();
        const Tombstones* edgeTombstones = tombstones.hasEdges() ? &tombstones : nullptr;

        PathTargetBatch& batch = _batches.emplace_back();
        buildSetBatch(parts, targets, direction, edgeTypes, edgeTombstones, maxHops, batch);
    } else {
        _set.build(view, targets, direction, edgeTypes, maxHops);
    }

    _built = true;
}

bool PathTargetIndex::isDense() const {
    return _set.isBuilt() || (!_batches.empty() && _batches.front().isDense());
}

size_t PathTargetIndex::getReachedCount() const {
    if (_set.isBuilt()) {
        return _set.getReachedCount();
    }

    size_t reached = 0;
    for (const PathTargetBatch& batch : _batches) {
        reached += batch.size();
    }

    return reached;
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
                                      std::span<const EdgeTypeID> edgeTypes,
                                      const PathDistanceIndex::SeedExpansion& expansion,
                                      size_t seedCount,
                                      size_t targetCount,
                                      uint64_t maxHops,
                                      double hopPassRate) {
    if (targetCount == 0) {
        return false;
    }

    const PartDirectory parts(view);
    if (parts.getAllocatedNodeCount() == 0 || parts.getAllocatedEdgeCount() == 0) {
        return false;
    }

    const size_t fullBatchCount = targetCount / targetsPerBatch;
    const size_t lastBatchSize = targetCount % targetsPerBatch;

    double checks = 0.0;
    double bytes = 0.0;
    if (fullBatchCount > 0) {
        BatchPlan full;
        planBatch(parts, direction, edgeTypes, targetsPerBatch, maxHops, full);
        checks += static_cast<double>(fullBatchCount) * full._checks;
        bytes += static_cast<double>(fullBatchCount) * full._bytes;
    }
    if (lastBatchSize > 0) {
        BatchPlan last;
        planBatch(parts, direction, edgeTypes, lastBatchSize, maxHops, last);
        checks += last._checks;
        bytes += last._bytes;
    }

    if (bytes > bytesLimit) {
        return false;
    }

    return PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, seedCount, maxHops, hopPassRate) > checks;
}

bool PathTargetIndex::isWorthBuildingSet(const GraphView& view,
                                         PathExplorationDir direction,
                                         std::span<const EdgeTypeID> edgeTypes,
                                         const PathDistanceIndex::SeedExpansion& expansion,
                                         size_t seedCount,
                                         size_t targetCount,
                                         uint64_t maxHops,
                                         double hopPassRate) {
    if (targetCount == 0) {
        return false;
    }

    const PartDirectory parts(view);
    const size_t nodeCount = parts.getAllocatedNodeCount();
    if (nodeCount == 0 || parts.getAllocatedEdgeCount() == 0) {
        return false;
    }

    SetPlan plan;
    planSet(parts, direction, edgeTypes, targetCount, maxHops, plan);
    if (plan._bytes > bytesLimit) {
        return false;
    }

    return PathDistanceIndex::estimatedEnumerationChecks(parts, expansion, seedCount, maxHops, hopPassRate) > plan._checks;
}

void PathTargetIndex::buildBatch(const PartDirectory& parts,
                                 std::span<const NodeID> targets,
                                 PathExplorationDir direction,
                                 std::span<const EdgeTypeID> edgeTypes,
                                 const Tombstones* tombstones,
                                 uint64_t maxHops,
                                 PathTargetBatch& batch) {
    std::vector<NodeID> frontier;
    for (size_t bit = 0; bit < targets.size(); bit++) {
        const NodeID target = targets[bit];
        if (target.getValue() >= parts.getAllocatedNodeCount()) {
            continue;
        }

        batch.gain(target, 1ull << bit, 0);
        if (!batch.isQueued(target)) {
            batch.setQueued(target, true);
            frontier.push_back(target);
        }

        _handles[target.getValue()] = PathTargetHandle {&batch, bit};
    }

    searchBatch(parts, frontier, direction, edgeTypes, tombstones, maxHops, batch);
}

// Every target of a set shares the one bit, so the word a node gains says it is in reach of
// the set and its first distance is the hops to the nearest target
void PathTargetIndex::buildSetBatch(const PartDirectory& parts,
                                    std::span<const NodeID> targets,
                                    PathExplorationDir direction,
                                    std::span<const EdgeTypeID> edgeTypes,
                                    const Tombstones* tombstones,
                                    uint64_t maxHops,
                                    PathTargetBatch& batch) {
    std::vector<NodeID> frontier;
    for (const NodeID target : targets) {
        if (target.getValue() >= parts.getAllocatedNodeCount()) {
            continue;
        }

        batch.gain(target, 1ull, 0);
        if (!batch.isQueued(target)) {
            batch.setQueued(target, true);
            frontier.push_back(target);
        }
    }

    searchBatch(parts, frontier, direction, edgeTypes, tombstones, maxHops, batch);
}

void PathTargetIndex::searchBatch(const PartDirectory& parts,
                                  std::vector<NodeID>& frontier,
                                  PathExplorationDir direction,
                                  std::span<const EdgeTypeID> edgeTypes,
                                  const Tombstones* tombstones,
                                  uint64_t maxHops,
                                  PathTargetBatch& batch) {
    // A frontier node's word is read as it stood when the level closed: two adjacent frontier
    // nodes would otherwise hand each other this level's bits one level too early
    std::vector<uint64_t> frontierWords;
    for (const NodeID target : frontier) {
        batch.setQueued(target, false);
        frontierWords.push_back(batch.getReached(target));
    }

    const bool walksIns = direction != PathExplorationDir::BACKWARD;
    const bool walksOuts = direction != PathExplorationDir::FORWARD;

    std::vector<NodeID> next;

    // A hop count saturates at the farthest one byte holds, so past that it reads "at least
    // that many hops". canReachWithin then keeps a node a deeper bound may not be able to
    // use, which costs a walk that never completes; capping the search instead would drop it.
    for (uint64_t level = 1; level <= maxHops && !frontier.empty(); level++) {
        const uint8_t distance = static_cast<uint8_t>(std::min<uint64_t>(level, PathTargetBatch::farthestLevel));
        batch.beginLevel();
        next.clear();

        for (size_t index = 0; index < frontier.size(); index++) {
            const NodeID node = frontier[index];
            const uint64_t word = frontierWords[index];
            const size_t owner = parts.ownerIndex(node);
            if (owner == parts.size()) {
                continue;
            }

            const EdgeIndexer& ownerIndexer = *parts.get(owner)._indexer;
            if (walksIns) {
                relax(ownerIndexer.getNodeInEdges(node), word, distance, edgeTypes, tombstones, batch, next);
            }
            if (walksOuts) {
                relax(ownerIndexer.getNodeOutEdges(node), word, distance, edgeTypes, tombstones, batch, next);
            }

            for (const size_t patchIndex : parts.patchPartsAfter(owner)) {
                const EdgeIndexer& patchIndexer = *parts.get(patchIndex)._indexer;
                if (walksIns) {
                    relax(patchIndexer.getNodeInEdges(node), word, distance, edgeTypes, tombstones, batch, next);
                }
                if (walksOuts) {
                    relax(patchIndexer.getNodeOutEdges(node), word, distance, edgeTypes, tombstones, batch, next);
                }
            }
        }

        frontierWords.clear();
        for (const NodeID node : next) {
            batch.setQueued(node, false);
            frontierWords.push_back(batch.getReached(node));
        }

        std::swap(frontier, next);
    }
}

void PathTargetIndex::relax(std::span<const EdgeRecord> edges,
                            uint64_t word,
                            uint8_t distance,
                            std::span<const EdgeTypeID> edgeTypes,
                            const Tombstones* tombstones,
                            PathTargetBatch& batch,
                            std::vector<NodeID>& next) {
    for (const EdgeRecord& record : edges) {
        const bool wrongType = !edgeTypes.empty() && !edgeTypeMatches(edgeTypes, record._edgeTypeID);
        const bool deleted = tombstones && tombstones->containsEdge(record._edgeID);
        if (wrongType || deleted) {
            continue;
        }

        const NodeID other = record._otherID;
        if (batch.gain(other, word, distance) == 0) {
            continue;
        }

        if (!batch.isQueued(other)) {
            batch.setQueued(other, true);
            next.push_back(other);
        }
    }
}
