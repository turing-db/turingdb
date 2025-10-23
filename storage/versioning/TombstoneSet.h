#pragma once

#include <ranges>
#include <unordered_set>

#include "ID.h"

namespace db {

/**
 * @brief Wrapper for datastructure which is used to store nodes/edges which are
 * implicitly deleted, and filter the output of ChunkWriters based on these deletions.
 * @detail Currently implemented using an unordered set, as latency-critical operation is
 * lookups when filtering the output of chunk writers. Set theoretic union is also
 * required - when rebasing - however this is less frequent. 
 */
template <TypedInternalID IDT>
class TombstoneSet {
public:
    bool contains(IDT id) const;

    void insert(IDT id);

    void reserve(size_t size);

    size_t size() const;
    bool empty() const { return _set.empty(); }

    auto begin() { return _set.begin(); }
    auto end() { return _set.end(); }
    auto begin() const { return _set.begin(); }
    auto end() const { return _set.end(); }

    void swap(TombstoneSet<IDT>& other) noexcept;

    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, IDT>
    void insert(Range& range);

    /**
     * @brief Performs set-theoretic union of @param set1 and @param set2, storing the
     * result in @param set1
     */
    static void setUnion(TombstoneSet<IDT>& set1, const TombstoneSet<IDT>& set2);

private:
    std::unordered_set<IDT> _set;
};

template <TypedInternalID IDT>
template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, IDT>
void TombstoneSet<IDT>::insert(Range& range) {
    _set.insert(std::ranges::begin(range), std::ranges::end(range));
}

}
