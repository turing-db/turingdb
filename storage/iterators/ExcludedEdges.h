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

    // How many distinct edges of @p excluded a node's out-run or in-run in @p part holds,
    // found by arithmetic on the out-run and by each edge's out-record for the in-run, so
    // that no record of the run itself is read.
    static size_t countInOutRun(std::span<const EdgeID> excluded, const EdgeContainer& part, std::span<const EdgeRecord> outRun);
    static size_t countInInRun(std::span<const EdgeID> excluded, const EdgeContainer& part, NodeID node);

    // Writes the run's candidates into the non-null columns from @p begin on, leaving out
    // the @p held edges of @p excluded that it holds, and returns the columns' new size.
    // @p outPart is the part whose out-edges the run is a slice of, null for an in-run: an
    // out-run's IDs are consecutive, so its excluded edges are located by arithmetic.
    static size_t copyRunLeavingOut(std::span<const EdgeID> excluded,
                                    size_t held,
                                    std::span<const EdgeRecord> run,
                                    size_t begin,
                                    const EdgeContainer* outPart,
                                    ColumnVector<size_t>* indices,
                                    ColumnEdgeIDs* edgeIDs,
                                    ColumnNodeIDs* others,
                                    ColumnEdgeTypes* types);
};

}
