#include "TombstoneFilter.h"

#include "versioning/Tombstones.h"
#include "columns/ColumnVector.h"
#include "ID.h"

using namespace db;

namespace db {
template void TombstoneFilter::populateDeletedIndices<NodeID>(const ColumnVector<NodeID>& column);
template void TombstoneFilter::populateDeletedIndices<EdgeID>(const ColumnVector<EdgeID>& column);
}

TombstoneFilter::TombstoneFilter(const Tombstones& tombstones)
        : _tombstones(tombstones)
{
}

template <TypedInternalID IDT>
void TombstoneFilter::populateDeletedIndices(const ColumnVector<IDT>& column) {
    if constexpr (std::is_same_v<IDT, NodeID>) {
        if (!_tombstones.hasNodes()) {
            return;
        }
    } else {
        if (!_tombstones.hasEdges()) {
            return;
        }
    }

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

