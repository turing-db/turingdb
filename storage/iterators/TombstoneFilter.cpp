#include "TombstoneFilter.h"

#include "versioning/Tombstones.h"
#include "columns/ColumnVector.h"
#include "ID.h"

using namespace db;

namespace db {
template void TombstoneFilter::populateDeletedIndices<NodeID>(const ColumnVector<NodeID>& column);
template void TombstoneFilter::populateDeletedIndices<EdgeID>(const ColumnVector<EdgeID>& column);

template void TombstoneFilter::onePassApplyFilter<NodeID>(ColumnVector<NodeID>& column);
template void TombstoneFilter::onePassApplyFilter<EdgeID>(ColumnVector<EdgeID>& column);
}

TombstoneFilter::TombstoneFilter(const Tombstones& tombstones)
        : _tombstones(tombstones)
{
}

template <TypedInternalID IDT>
void TombstoneFilter::populateDeletedIndices(const ColumnVector<IDT>& column) {
    for (size_t i = 0; i < column.size(); i++) {
        const IDT id = column.getRaw()[i];

        bool deleted {false};
        if constexpr (std::is_same_v<IDT, NodeID>) {
            deleted = _tombstones.containsNode(id);
        } else {
            deleted = _tombstones.containsEdge(id);
        }

        if (deleted) {
            _deletedIndices.insert(i);
        }
    }
}

template <TypedInternalID IDT>
void TombstoneFilter::onePassApplyFilter(ColumnVector<IDT>& column) {
    size_t initialSize = column.size();
    std::vector<IDT>& raw = column.getRaw();

    // 2 pointer approach: traverse the vector with @ref readPointer and
    // @ref writePointer. Overwrite values at @ref writePointer with the value at
    // @ref readPointer if the index of @ref readPointer is not to be deleted.
    // Always increment read, only increment write if the value isn't deleted
    size_t writePointer = 0;
    for (size_t readPointer = 0; readPointer < initialSize; readPointer++) {
        bool deleted {false};
        if constexpr (std::is_same_v<IDT, NodeID>) {
            deleted = _tombstones.containsNode(raw[readPointer]);
        } else {
            deleted = _tombstones.containsEdge(raw[readPointer]);
        }
        if (!deleted) {
            raw[writePointer] = raw[readPointer];
            writePointer++;
            continue;
        }
    }
    raw.resize(writePointer);
}
