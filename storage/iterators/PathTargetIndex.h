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

// Where one target sits in a PathTargetIndex: the word arrays of its batch and its bit. An
// invalid handle stands for a target the index does not cover and prunes nothing.
struct PathTargetHandle {
    using Levels = std::vector<std::vector<uint64_t>>;

    const Levels* _levels {nullptr};
    uint64_t _mask {0};

    bool isValid() const { return _levels != nullptr; }

    bool canReachWithin(NodeID node, uint64_t hops) const {
        if (!_levels) {
            return true;
        }

        const size_t last = _levels->size() - 1;
        const std::vector<uint64_t>& reached = (*_levels)[hops < last ? hops : last];
        const size_t index = node.getValue();

        return index < reached.size() && (reached[index] & _mask) != 0;
    }
};

// For every node and each of a chunk's distinct targets, whether the target is within L hops
// along the exploration direction, for every L up to the hop bound: one multi-source
// breadth-first search per 64 targets over the reverse direction, keeping a cumulative word
// per node per level. Like PathDistanceIndex it ignores trail uniqueness and hop predicates,
// so pruning by it never drops a valid path.
class PathTargetIndex {
public:
    static constexpr size_t targetsPerBatch = 64;

    PathTargetIndex();
    ~PathTargetIndex();

    void build(const GraphView& view,
               std::span<const NodeID> targets,
               PathExplorationDir direction,
               std::optional<EdgeTypeID> edgeType,
               uint64_t maxHops);

    bool isBuilt() const { return _built; }
    size_t getBatchCount() const { return _batches.size(); }

    PathTargetHandle find(NodeID target) const;

    // Whether the enumeration the seeds imply is expected to cost more than the batches
    static bool isWorthBuilding(const GraphView& view,
                                PathExplorationDir direction,
                                size_t seedCount,
                                size_t targetCount,
                                uint64_t maxHops);

private:
    std::vector<PathTargetHandle::Levels> _batches;
    std::unordered_map<uint64_t, PathTargetHandle> _handles;
    bool _built {false};

    void buildBatch(const PartDirectory& parts,
                    std::span<const NodeID> targets,
                    PathExplorationDir direction,
                    std::optional<EdgeTypeID> edgeType,
                    const Tombstones* tombstones,
                    uint64_t levelCap,
                    PathTargetHandle::Levels& levels);
    void relax(std::span<const EdgeRecord> edges,
               uint64_t word,
               std::optional<EdgeTypeID> edgeType,
               const Tombstones* tombstones,
               std::vector<uint64_t>& reached,
               std::vector<uint8_t>& queued,
               std::vector<NodeID>& next);
};

}
