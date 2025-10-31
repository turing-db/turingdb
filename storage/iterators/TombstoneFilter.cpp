#include "TombstoneFilter.h"

#include <memory>
#include <cstring>

#include "versioning/Tombstones.h"
#include "columns/ColumnVector.h"
#include "ID.h"

#include "BioAssert.h"

using namespace db;

namespace db {

template void TombstoneFilter::populateRanges<NodeID>(const ColumnVector<NodeID>* baseCol);
template void TombstoneFilter::populateRanges<EdgeID>(const ColumnVector<EdgeID>* baseCol);

}

TombstoneFilter::TombstoneFilter(const Tombstones& tombstones)
    : _tombstones(tombstones)
{
}

template <TypedInternalID IDT>
void TombstoneFilter::populateRanges(const ColumnVector<IDT>* baseCol) {
    bioassert(baseCol);
    // TODO: Throw FatalException if !baseCol

    // We use pointer indirection to avoid heap allocating a vector in each chunk writer
    // if the filter is never needed. On first invocation of this function, create the
    // vector.
    if (!_nonDeletedRanges) {
        _nonDeletedRanges = std::make_unique<NonDeletedRanges>();
    }

    // Failsafe
    _nonDeletedRanges->clear();

    const ColumnVector<IDT>& col = *baseCol;

    size_t i = 0;
    while (i < col.size()) { // Scan each element in the base column
        // Skip until we find a non-deleted entry
        bool deleted =  _tombstones.contains(col[i]);
        if (deleted) {
            i++;
            continue;
        }

        // We have found a non-deleted entry, scan forward to see how many non-deleted
        // entries we have in a row, and form a range from it
        size_t start = i;
        size_t size = 1;
        i++; // Pre-increment i: we scan the next entry after the non-deleted just found
        while (i < col.size() && !_tombstones.contains(col[i])) {
            i++;
            size++;
        }

        _nonDeletedRanges->emplace_back(start, size);
    }
}

