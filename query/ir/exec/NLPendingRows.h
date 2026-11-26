#pragma once

#include <stdint.h>

#include "ID.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

#include "NLRuntimeProperty.h"

namespace db {

// A column a merge produced mixes the two ID spaces, so its mask answers row by row;
// one with no mask is pending in every row or in none.
inline bool isPendingRow(const ColumnVector<CustomBool>* pending, bool isPendingColumn, size_t row) {
    if (pending) {
        return (*pending)[row];
    }

    return isPendingColumn;
}

// Which rows of an ID column name an entity this change has written and not committed. A
// column a write bound says so through the mask or the flag codegen put on the op; a hop
// produces its own rows, and the ID is what tells the two spaces apart there - this
// change's own start where the graph's end.
class PendingRows {
public:
    PendingRows(const ColumnVector<CustomBool>* pending, bool isPendingColumn, size_t committedCount, size_t pendingCount)
        : _pending(pending),
        _isPendingColumn(isPendingColumn),
        _committedCount(committedCount),
        _pendingCount(pendingCount)
    {
    }

    bool has(size_t row, uint64_t id) const {
        if (id == EntityID::max().getValue()) {
            return false;
        }

        const bool marked = isPendingRow(_pending, _isPendingColumn, row);
        const bool namesWritten = id >= _committedCount && id - _committedCount < _pendingCount;

        return marked || namesWritten;
    }

private:
    const ColumnVector<CustomBool>* _pending {nullptr};
    bool _isPendingColumn {false};
    size_t _committedCount {0};
    size_t _pendingCount {0};
};

}
