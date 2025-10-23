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
void TombstoneSet<IDT>::insert(IDT id) {
    _set.insert(id);
}

template <TypedInternalID IDT>
void TombstoneSet<IDT>::reserve(size_t size) {
    return _set.reserve(size);
}

template <TypedInternalID IDT>
size_t TombstoneSet<IDT>::size() const {
    return _set.size();
}

template <TypedInternalID IDT>
void TombstoneSet<IDT>::swap(TombstoneSet<IDT>& other) noexcept {
    _set.swap(other._set);
};

template <TypedInternalID IDT>
void TombstoneSet<IDT>::setUnion(TombstoneSet<IDT>& set1, const TombstoneSet<IDT>& set2) {
    set1.insert(set2);
}
