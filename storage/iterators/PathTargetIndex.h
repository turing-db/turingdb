#pragma once

#include <optional>
#include <span>
#include <stdint.h>
#include <stddef.h>
#include <unordered_map>
#include <vector>

#include "PathExplorationDir.h"
#include "datapart/EdgeRecord.h"
#include "views/GraphView.h"
#include "ID.h"

namespace db {

class PartDirectory;
class Tombstones;

// The nodes one batch of up to 64 targets reaches, in one of two layouts. Sparse: an
// open-addressing table keyed by node holding the word of the targets that reached it and a
// hop count per target, so a batch costs its ball. Dense: one cumulative word per node per
// level, the targets within that many hops, so a batch costs the graph but each word is a
// sequential write; it wins when the ball is a large share of a small graph at a shallow bound.
class PathTargetBatch {
public:
    static constexpr size_t targetsPerBatch = 64;
    static constexpr uint8_t unreached = 255;

    PathTargetBatch();
    ~PathTargetBatch();

    // Lays the batch out densely over that many nodes; the default is the sparse table
    void setDense(size_t nodeCount);
    bool isDense() const { return _dense; }

    size_t size() const { return _count; }

    bool canReachWithin(NodeID node, size_t bit, uint64_t hops) const {
        if (_dense) {
            const size_t last = _levels.size() - 1;
            const std::vector<uint64_t>& reached = _levels[hops < last ? hops : last];
            const size_t index = node.getValue();

            return index < reached.size() && ((reached[index] >> bit) & 1) != 0;
        }

        const size_t slot = find(node);
        if (slot == _keys.size()) {
            return false;
        }

        const uint8_t distance = _distances[slot * targetsPerBatch + bit];

        return distance != unreached && distance <= hops;
    }

    // The targets that have reached the node so far
    uint64_t getReached(NodeID node) const;

    // Adds to the node the targets of the word it did not have yet, at that many hops, and
    // returns them
    uint64_t gain(NodeID node, uint64_t word, uint8_t distance);

    bool isQueued(NodeID node) const;
    void setQueued(NodeID node, bool queued);

    // Opens the next level: the dense layout starts it as a copy of the one that closed
    void beginLevel();

private:
    static constexpr uint64_t emptyKey = ~0ull;
    static constexpr size_t initialCapacity = 128;

    bool _dense {false};
    size_t _count {0};

    std::vector<std::vector<uint64_t>> _levels;
    std::vector<uint8_t> _denseQueued;

    std::vector<uint64_t> _keys;
    std::vector<uint64_t> _reached;
    std::vector<uint8_t> _distances;
    std::vector<uint8_t> _queued;
    uint64_t _mask {0};

    static uint64_t hashOf(uint64_t key) { return (key * 0x9E3779B97F4A7C15ull) >> 20; }

    size_t find(NodeID node) const {
        const uint64_t key = node.getValue();
        size_t slot = hashOf(key) & _mask;

        while (_keys[slot] != emptyKey) {
            if (_keys[slot] == key) {
                return slot;
            }

            slot = (slot + 1) & _mask;
        }

        return _keys.size();
    }

    size_t findOrInsert(NodeID node);
    void allocate(size_t capacity);
    void grow();
};

// Where one target sits in a PathTargetIndex: its batch and its bit. An invalid handle stands
// for a target the index does not cover and prunes nothing.
struct PathTargetHandle {
    const PathTargetBatch* _batch {nullptr};
    size_t _bit {0};

    bool isValid() const { return _batch != nullptr; }

    bool canReachWithin(NodeID node, uint64_t hops) const {
        return !_batch || _batch->canReachWithin(node, _bit, hops);
    }
};

// For each of a chunk's distinct targets, the hops every node it reaches needs to get to it
// along the exploration direction: one multi-source breadth-first search per 64 targets over
// the reverse direction. Like PathDistanceIndex it ignores trail uniqueness and hop
// predicates, so pruning by it never drops a valid path.
class PathTargetIndex {
public:
    static constexpr size_t targetsPerBatch = PathTargetBatch::targetsPerBatch;

    PathTargetIndex();
    ~PathTargetIndex();

    void build(const GraphView& view,
               std::span<const NodeID> targets,
               PathExplorationDir direction,
               std::optional<EdgeTypeID> edgeType,
               uint64_t maxHops);

    bool isBuilt() const { return _built; }
    bool isDense() const;
    size_t getBatchCount() const { return _batches.size(); }
    size_t getReachedCount() const;

    PathTargetHandle find(NodeID target) const;

    // Whether the enumeration the seeds imply is expected to cost more than the batches
    static bool isWorthBuilding(const GraphView& view,
                                PathExplorationDir direction,
                                std::optional<EdgeTypeID> edgeType,
                                size_t seedCount,
                                size_t targetCount,
                                uint64_t maxHops);

private:
    // What one batch of the build is expected to cost in the cheaper of the two layouts
    struct BatchPlan {
        bool _dense {false};
        double _checks {0.0};
        double _bytes {0.0};
    };

    std::vector<PathTargetBatch> _batches;
    std::unordered_map<uint64_t, PathTargetHandle> _handles;
    bool _built {false};

    static void planBatch(const PartDirectory& parts,
                          PathExplorationDir direction,
                          std::optional<EdgeTypeID> edgeType,
                          uint64_t maxHops,
                          BatchPlan& plan);

    void buildBatch(const PartDirectory& parts,
                    std::span<const NodeID> targets,
                    PathExplorationDir direction,
                    std::optional<EdgeTypeID> edgeType,
                    const Tombstones* tombstones,
                    uint64_t levelCap,
                    PathTargetBatch& batch);
    void relax(std::span<const EdgeRecord> edges,
               uint64_t word,
               uint8_t distance,
               std::optional<EdgeTypeID> edgeType,
               const Tombstones* tombstones,
               PathTargetBatch& batch,
               std::vector<NodeID>& next);
};

}
