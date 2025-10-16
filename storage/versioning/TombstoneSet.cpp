#include "TombstoneSet.h"

#include "ID.h"

using namespace db;

namespace db {
    template class TombstoneSet<NodeID>;
    template class TombstoneSet<EdgeID>;
}

template <TypedInternalID IDT>
bool TombstoneSet<IDT>::contains(IDT id) const {
    return _set.contains(id);
}

template <TypedInternalID IDT>
auto TombstoneSet<IDT>::insert(IDT id) {
    return _set.insert(id);
}

template <TypedInternalID IDT>
size_t TombstoneSet<IDT>::size() const {
    return _set.size();
}

