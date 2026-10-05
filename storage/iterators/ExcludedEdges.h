#pragma once

#include <span>
#include <stddef.h>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "ID.h"

namespace db {

class EdgeContainer;

// The edges each input row of an expansion may not repeat, one span per row: row r's are
// _edges[_offsets[r] .. _offsets[r + 1]). Empty when the expansion excludes nothing.
struct ExcludedEdges {
    std::span<const size_t> _offsets;
    std::span<const EdgeID> _edges;

    bool isSet() const { return !_offsets.empty(); }

    std::span<const EdgeID> rowEdges(size_t row) const {
        return _edges.subspan(_offsets[row], _offsets[row + 1] - _offsets[row]);
    }

    static bool holds(std::span<const EdgeID> edges, EdgeID edge);
    static bool repeatsEarlier(std::span<const EdgeID> edges, size_t position);

    // Row @p row's excluded edges, empty when the expansion excludes nothing. At the start of
    // the row's run, @p held is reset to how many distinct ones the out-run or in-run holds.
    std::span<const EdgeID> enterOutRun(size_t row, bool runStarts, const EdgeContainer& part, std::span<const EdgeRecord> outRun, size_t& held) const;
    std::span<const EdgeID> enterInRun(size_t row, bool runStarts, const EdgeContainer& part, NodeID node, size_t& held) const;

    // Whether a run's edge is one of @p excluded, counted off @p held when it is
    static bool leavesOut(std::span<const EdgeID> excluded, EdgeID edge, size_t& held);

    // With only indices written every row of a run is the same, so the rows its held edges
    // would have produced come off the slice's end: how many, counted off @p held
    static size_t dropHeld(size_t sliceSize, size_t& held);

    // Writes the run's candidates into the non-null columns from @p begin on, leaving out the
    // edges of @p excluded it holds, and returns how many it left out, counted off @p held.
    // @p outPart is the part an out-run is a slice of, whose consecutive IDs locate its
    // excluded edges by arithmetic; null for an in-run.
    static size_t copyRunLeavingOut(std::span<const EdgeID> excluded,
                                    size_t& held,
                                    std::span<const EdgeRecord> run,
                                    size_t begin,
                                    const EdgeContainer* outPart,
                                    ColumnVector<size_t>* indices,
                                    ColumnEdgeIDs* edgeIDs,
                                    ColumnNodeIDs* others,
                                    ColumnEdgeTypes* types);

private:
    // How many distinct edges of @p excluded a node's out-run or in-run in @p part holds,
    // found by arithmetic on the out-run and by each edge's out-record for the in-run, so
    // that no record of the run itself is read.
    static size_t countInOutRun(std::span<const EdgeID> excluded, const EdgeContainer& part, std::span<const EdgeRecord> outRun);
    static size_t countInInRun(std::span<const EdgeID> excluded, const EdgeContainer& part, NodeID node);
};

}
