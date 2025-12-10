#pragma once

#include "versioning/Tombstones.h"
#include "versioning/NonDeletedRanges.h"
#include "columns/ColumnVector.h"
#include "ID.h"

#include "BioAssert.h"

namespace db {

class Tombstones;

template <typename T>
class ColumnVector;

/**
 * @brief Provides generic methods for applying filtering based on tombstones to the
 * ColumnVector outputs of a ChunkWriter.
 * @detail Uses a two-stage, multipass filtering approach based on a "base column" which
 * is determined by the ChunkWriter whose output is being filtered.
 * Stage 1: Call @ref populateRanges to find the intervals in the base column which are
 * not deleted
 * Stage 2: Calls to @ref filter for each column to be filtered, which moves all
 * non-deleted ranges to the front of the column - removing deleted rows
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

    /**
     * @brief Moves ranges in @ref _nonDeletedRanges to the front of the column, and
     * truncates any remaining rows
     * @warn Requires @ref populateRanges to be called prior to populate
     * @ref _nonDeletedRanges
     */
    template <typename T>
    void filter(ColumnVector<T>* col);

    void reset() {
        _initialised = false;
        if (_nonDeletedRanges) {
            _nonDeletedRanges->clear();
        }
    }

private:
    const Tombstones& _tombstones;

    // Pointer indirection to vector: only allocate vector if the owning ChunkWriter needs
    // to be filter
    std::unique_ptr<NonDeletedRanges> _nonDeletedRanges;

    bool _initialised {false}; // Tracks whether @ref populateRanges was called
};

template <typename T>
void TombstoneFilter::filter(ColumnVector<T>* col) {
    bioassert(col, "Column must be valid");
    bioassert(_initialised, "TombstoneFilter must be initialised");
    bioassert(_nonDeletedRanges, "TombstoneFilter must have _nonDeletedRanges");

    if (!col) [[unlikely]] {
        return;
    }

    // No non-deleted entries => all deleted => clear
    if (_nonDeletedRanges->empty()) {
        col->clear();
        return;
    }

    auto* data = col->getRaw().data();

    // Keep track of the index we are writing to
    size_t writePtr = 0;
    for (const NonDeletedRange& rg : *_nonDeletedRanges) {
        // Shift the non-deleted range left, to the current write position
        std::memmove(data + writePtr, data + rg.start, sizeof(T) * rg.size);
        writePtr += rg.size;
    }

    // Truncate the end of the column: anything remaining is either deleted or has already
    // been shifted to an index < writePtr
    col->resize(writePtr);
}

}
