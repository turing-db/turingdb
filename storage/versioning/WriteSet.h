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

    void finalise();

    static bool emptyIntersection(const WriteSet<IDT>& set1, const WriteSet<IDT>& set2);

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
