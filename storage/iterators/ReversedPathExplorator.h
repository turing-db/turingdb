#pragma once

#include <span>
#include <stdint.h>
#include <stddef.h>
#include <vector>

#include "ChunkWriter.h"
#include "PartDirectory.h"
#include "PathDistanceIndex.h"
#include "PathExplorationDir.h"
#include "PathExplorator.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "metadata/LabelSet.h"

namespace db {

class PathTargetIndex;

// The rows of an exploration ending on a set of nodes, enumerated by walking from the set back
// to the seeds: a trail from an end to a seed is the row of every input row holding that seed,
// (input row, end node), as the walk from the seeds emits it. It writes no path, since the
// trail it holds runs from the end.
class ReversedPathExplorator {
public:
    ReversedPathExplorator(const GraphView& view,
                           const ColumnNodeIDs* inputNodeIDs,
                           std::span<const NodeID> endNodeSet,
                           const LabelSet* endLabels,
                           PathExplorationDir direction,
                           uint64_t minHops,
                           uint64_t maxHops);
    ~ReversedPathExplorator();

    // Whether the walk from the end set is expected to examine fewer edges than the walk from
    // the seeds. What a walk costs turns on the fan-out where it starts, so each side's own
    // nodes are sampled rather than the graph's average; @param fromEnds is the end set's.
    static bool isCheaper(const PartDirectory& parts,
                          PathExplorationDir direction,
                          std::span<const EdgeTypeID> edgeTypes,
                          std::span<const NodeID> seeds,
                          std::span<const NodeID> endNodeSet,
                          uint64_t maxHops,
                          PathDistanceIndex::SeedExpansion& fromEnds);

    void setIndices(ColumnVector<size_t>* indices) { _indices = indices; }
    void setTargets(ColumnNodeIDs* targets) { _targets = targets; }
    void setEdgeTypeFilter(std::span<const EdgeTypeID> edgeTypes);
    void setDistinctEnds(bool distinct);
    void setTargetIndex(const PathTargetIndex* index);

    // The seeds the walk heads for, sorted and without duplicates
    std::span<const NodeID> getSeedNodes() const { return _seedNodes; }

    void fill(size_t maxCount);
    bool isValid() const;

private:
    struct SeedRow {
        NodeID _node;
        size_t _row {0};
    };

    ColumnNodeIDs _ends;
    std::vector<NodeID> _seedNodes;
    std::vector<SeedRow> _seedRows;
    ColumnVector<size_t> _walkIndices;
    ColumnNodeIDs _walkTargets;
    PathExplorator _explorator;

    ColumnVector<size_t>* _indices {nullptr};
    ColumnNodeIDs* _targets {nullptr};

    // The next row of the walk's last fill to emit, and the range of _seedRows it still owes
    size_t _walkRow {0};
    size_t _seedRowCursor {0};
    size_t _seedRowEnd {0};

    static bool isBefore(const SeedRow& lhs, const SeedRow& rhs);

    void startWalkRow();
};

static_assert(NonRootChunkWriter<ReversedPathExplorator>);

}
