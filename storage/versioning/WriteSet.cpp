#include "WriteSet.h"

#include "BioAssert.h"
#include <algorithm>

using namespace db;

namespace db {
    template class WriteSet<NodeID>;
    template class WriteSet<EdgeID>;
}

template <TypedInternalID IDT>
bool WriteSet<IDT>::contains(IDT id) {
    bioassert(std::ranges::is_sorted(_set));
    return std::ranges::binary_search(_set, id);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::insert(IDT id) {
    _set.push_back(id);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::finalise() {
    std::ranges::sort(_set);
    auto [newEnd, oldEnd] = std::ranges::unique(_set);
    _set.erase(newEnd, oldEnd);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::setUnion(WriteSet<IDT>& set1, const WriteSet<IDT>& set2) {
    bioassert(std::ranges::is_sorted(set1));
    bioassert(std::ranges::is_sorted(set2));
    bioassert(std::ranges::adjacent_find(set1) == set1.end());
    bioassert(std::ranges::adjacent_find(set2) == set2.end());

    WriteSet<IDT> temp;

    std::set_union(set1.begin(), set1.end(),
                   set2.begin(), set2.end(),
                   std::back_inserter(temp));

    set1.swap(temp);
}

