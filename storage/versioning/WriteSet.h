#pragma once

#include "ID.h"

namespace db {

/**
 * @brief Wrapper class for a "set" which need support fast lookups, union, and
 * intersection as a member of @ref CommitJournal for conflict checking
 * @detail Primary usage is during conflict checking. This operation involves set union
 * and determining if the intersection is empty. Sorted vector is therefore chosen as it
 * is faster than a hashtable-based datastructure. Source:
 * https://lemire.me/blog/2017/01/27/how-expensive-are-the-union-and-intersection-of-two-unordered_set-in-c/
 */
template <TypedInternalID IDT>
class WriteSet {
public:
    bool contains(IDT id) const;

    void insert(IDT id);

    template <std::ranges::input_range Range>
        requires std::same_as<std::ranges::range_value_t<Range>, IDT>
    void insert(Range&& range);

    void clear() { _set.clear(); }

    size_t size() const { return _set.size(); }

    bool empty() const { return _set.empty(); }

    auto begin() { return _set.begin(); }
    auto end() { return _set.end(); }

    auto begin() const { return _set.begin(); }
    auto end() const { return _set.end(); }

    void swap(WriteSet<IDT>& other) noexcept;

    /**
    * @brief Finalises the journal for use in a frozen commit.
    * @detail Sorts and ensures uniqueness of elements.
    */
    void finalise();

    /**
    * @brief Determines if the intersection between @param set1 and @param set2 is empty
    */
    static bool emptyIntersection(const WriteSet<IDT>& set1, const WriteSet<IDT>& set2);

    /**
     * @brief Performs set-theoretic union of @param set1 and @param set2, storing the
     * result in @param set1.
     * @warn Requires both @param set1 and @param set2 to be sorted and unique.
     */
    static void setUnion(WriteSet<IDT>& set1, const WriteSet<IDT>& set2);

    static void setIntersection(WriteSet<IDT>& set1, WriteSet<IDT>& set2) = delete;

private:
    friend class CommitJournal;

    std::vector<IDT> _set;

    std::vector<IDT>& getRaw() { return _set; }
};

template <TypedInternalID IDT>
template <std::ranges::input_range Range>
    requires std::same_as<std::ranges::range_value_t<Range>, IDT>
void WriteSet<IDT>::insert(Range&& range) {
    _set.insert(_set.begin(), std::ranges::begin(range), std::ranges::end(range));
}

struct ConflictCheckSets {
    WriteSet<NodeID> writtenNodes;
    WriteSet<EdgeID> writtenEdges;
};

}
