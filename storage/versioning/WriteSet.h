#pragma once

#include "ID.h"

namespace db {

template <TypedInternalID IDT>
class WriteSet {
public:
    bool contains(IDT id);

    void insert(const std::vector<IDT>& elements);

    void clear() { _set.clear(); }

    bool empty() { return _set.empty(); }

    static bool emptyIntersection(const WriteSet<IDT>& set1,
                                  const WriteSet<IDT>& set2) = delete;

    static WriteSet<IDT> setUnion(const WriteSet<IDT>& set1,
                                  const WriteSet<IDT>& set2) = delete;

    static WriteSet<IDT> setIntersection(const WriteSet<IDT>& set1,
                                         const WriteSet<IDT>& set2) = delete;

private:
    std::vector<IDT> _set;
};

template <TypedInternalID IDT>
bool WriteSet<IDT>::contains(IDT id) {
    bioassert(std::ranges::is_sorted(_set));
    return std::ranges::binary_search(_set, id);
}

template <TypedInternalID IDT>
void WriteSet<IDT>::insert(const std::vector<IDT>& elements) {
    _set.reserve(_set.size() + elements.size());
    _set.insert(_set.end(), elements.begin(), elements.end());
}
}
