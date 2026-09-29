#pragma once

#include <span>
#include <stddef.h>

#include "columns/ColumnEdgeTypes.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "datapart/EdgeRecord.h"
#include "ID.h"

namespace db {

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

    // Drops from the columns a writer filled from @p begin on the candidates of the run that
    // @p excluded holds, and returns the columns' new size; a null column is one the writer
    // does not fill. A node's out-edges carry consecutive IDs within a part, so a run of them
    // is searched by arithmetic and no record is read; any other run is scanned.
    static size_t pruneRun(std::span<const EdgeID> excluded,
                           std::span<const EdgeRecord> run,
                           size_t begin,
                           bool consecutiveIDs,
                           ColumnVector<size_t>* indices,
                           ColumnEdgeIDs* edgeIDs,
                           ColumnNodeIDs* others,
                           ColumnEdgeTypes* types);
};

}
