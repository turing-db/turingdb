#pragma once

#include <ranges>
#include <unordered_set>

#include "ID.h"

namespace db {

template <TypedInternalID IDT>
class TombstoneSet {
public:
    bool contains(IDT id) const;
    auto insert(IDT id);
    size_t size() const;

    bool empty() const { return _set.empty(); }

    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, IDT>
    void insert(Range& range);

private:
    std::unordered_set<IDT> _set;
};

template <TypedInternalID IDT>
template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, IDT>
void TombstoneSet<IDT>::insert(Range& range) {
    // Justification of using @ref insert here: union of two unordered_sets may be slower
    // than sorted vector, but the lookup for u_set is O(1) whilst sorted vec is O(logn).
    // Therefore, using u_set is better for reads, but worse for writes, which aligns
    // better to our philosphy than using sorted vec. (Source:
    // https://lemire.me/blog/2017/01/27/how-expensive-are-the-union-and-intersection-of-two-unordered_set-in-c/)
    _set.insert(std::ranges::begin(range), std::ranges::end(range));
}

}
