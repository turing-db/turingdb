#pragma once

#include "versioning/Tombstones.h"
#include "columns/ColumnVector.h"
#include "ID.h"

namespace db {

class Tombstones;

template <typename T>
class ColumnVector;

/**
 * @brief Provides generic methods for applying filtering based on tombstones to the
 * ColumnVector outputs of a ChunkWriter
 */
class TombstoneFilter {
public:
    TombstoneFilter(const Tombstones& tombstones);
    ~TombstoneFilter() = default;

    TombstoneFilter(const TombstoneFilter&) = delete;
    TombstoneFilter(TombstoneFilter&&) = delete;
    TombstoneFilter& operator=(const TombstoneFilter&) = delete;
    TombstoneFilter& operator=(TombstoneFilter&&) = delete;

    using ColumnIndices = ColumnVector<size_t>;

    /**
     * @brief Given a base column, stores the ranges of non-deleted runs of entries into
     * @ref _nonDeletedRanges
     */
    template <TypedInternalID IDT>
    void populateRanges(const ColumnVector<IDT>* baseCol);

    template <typename T>
    void filter(ColumnVector<T>* col);

private:
    struct NonDeletedRange;
    using NonDeletedRanges = std::vector<NonDeletedRange>;

    struct NonDeletedRange {
        size_t start;
        size_t size;
    };

    const Tombstones& _tombstones;
    NonDeletedRanges _nonDeletedRanges;
    bool _initialised {false};
};

template <typename T>
void TombstoneFilter::filter(ColumnVector<T>* col) {
    bioassert(col);
    bioassert(_initialised);
    // No non-deleted entries => all deleted => clear
    if (_nonDeletedRanges.empty()) {
        col->clear();
        return;
    }

    auto* data = col->getRaw().data();

    // Keep track of the index we are writing to
    size_t writePtr = 0;
    for (NonDeletedRange rg : _nonDeletedRanges) {
        // Shift the non-deleted range left, to the current write position
        std::memmove(data + writePtr, data + rg.start, sizeof(T) * rg.size);
        writePtr += rg.size;
    }

    // Truncate the end of the column: anything remaining is either deleted or has already
    // been shifted to an index < writePtr
    col->resize(writePtr);
}

}
