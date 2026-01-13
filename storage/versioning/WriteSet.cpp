#include "WriteSet.h"

#include <algorithm>

namespace db {

template <TypedInternalID IDT>
bool WriteSet<IDT>::contains(IDT id) const {
    return std::ranges::binary_search(_set, id);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::insert(IDT id) {
    _set.push_back(id);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::swap(WriteSet<IDT>& other) noexcept {
    this->_set.swap(other._set);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::finalise() {
    std::ranges::sort(_set);
    auto [newEnd, oldEnd] = std::ranges::unique(_set);
    _set.erase(newEnd, oldEnd);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::setUnion(WriteSet<IDT>& set1, const WriteSet<IDT>& set2) {
    WriteSet<IDT> temp;

    std::set_union(set1.begin(), set1.end(),
                   set2.begin(), set2.end(),
                   std::back_inserter(temp._set));

    set1.swap(temp);
}

template <TypedInternalID IDT>
bool WriteSet<IDT>::emptyIntersection(const WriteSet<IDT>& set1, const WriteSet<IDT>& set2) {
    auto it1 = set1.begin();
    auto end1 = set1.end();
    auto it2 = set2.begin();
    auto end2 = set2.end();

    while (it1 != end1 && it2 != end2) {
        if (*it1 < *it2) {
            ++it1;
        } else if (*it2 < *it1) {
            ++it2;
        } else {
            return false;
        }
    }

    return true;
}

// Explicit template instantiations
template class WriteSet<NodeID>;
template class WriteSet<EdgeID>;

} // namespace db

