#pragma once

#include <unordered_set>

#include "columns/ColumnVector.h"
#include "ID.h"

namespace db {

class Tombstones;

template <typename T>
class ColumnVector;

class TombstoneFilter {
public:
    TombstoneFilter(const Tombstones& tombstones);

    TombstoneFilter(const TombstoneFilter&) = delete;
    TombstoneFilter(TombstoneFilter&&) = delete;
    TombstoneFilter& operator=(const TombstoneFilter&) = delete;
    TombstoneFilter& operator=(TombstoneFilter&&) = delete;

    template <TypedInternalID IDT>
    void populateDeletedIndices(const ColumnVector<IDT>& column);

    template <typename T>
    void applyFilter(ColumnVector<T>& column);

    bool empty() const { return _deletedIndices.empty(); }

private:
    using DeletedIndices = std::unordered_set<size_t>;

    const Tombstones& _tombstones;
    DeletedIndices _deletedIndices;
};

template <typename T>
void TombstoneFilter::applyFilter(ColumnVector<T>& column) {
    size_t initialSize = column.size();
    std::vector<T>& raw = column.getRaw();

    // 2 pointer approach: traverse the vector with @ref readPointer and
    // @ref writePointer. Overwrite values at @ref writePointer with the value at
    // @ref readPointer if the index of @ref readPointer is not to be deleted.
    // Always increment read, only increment write if the value isn't deleted
    size_t writePointer = 0;
    for (size_t readPointer = 0; readPointer < initialSize; readPointer++) {
        if (!_deletedIndices.contains(readPointer)) {
            raw[writePointer] = raw[readPointer];
            writePointer++;
            continue;
        }
    }
    raw.resize(writePointer);
}

}
