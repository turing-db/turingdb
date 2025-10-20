#pragma once

#include "ID.h"

namespace db {

/**
 * @brief Wrapper class for a "set" which need support fast lookups, union, and
 * intersection as a member of @ref CommitJournal for conflict checking
 */
template <TypedInternalID IDT>
class WriteSet {
public:
    bool contains(IDT id);

    void insert(IDT id);

    void insert(const std::vector<IDT>& elements);

    void clear() { _set.clear(); }

    bool empty() { return _set.empty(); }

    auto begin() { return _set.begin(); }
    auto end() { return _set.end(); }

    void finalise();

    static bool emptyIntersection(const WriteSet<IDT>& set1,
                                  const WriteSet<IDT>& set2) = delete;

    static WriteSet<IDT> setUnion(const WriteSet<IDT>& set1,
                                  const WriteSet<IDT>& set2) = delete;

    static WriteSet<IDT> setIntersection(const WriteSet<IDT>& set1,
                                         const WriteSet<IDT>& set2) = delete;

private:
    friend class CommitJournalLoader;

    std::vector<IDT> _set;
};

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
void WriteSet<IDT>::insert(const std::vector<IDT>& elements) {
    _set.reserve(_set.size() + elements.size());
    _set.insert(_set.end(), elements.begin(), elements.end());
}

template <TypedInternalID IDT>
void WriteSet<IDT>::finalise() {
    std::ranges::sort(_set);
    auto [newEnd, oldEnd] = std::ranges::unique(_set);
    _set.erase(newEnd, oldEnd);
}

}
