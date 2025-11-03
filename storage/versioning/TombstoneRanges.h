#pragma once

#include "versioning/NonDeletedRanges.h"
#include "ID.h"

namespace db {
class Tombstones;
class EdgeRecord;

class TombstoneRanges {
public:
    using NonDeletedRangesIt = NonDeletedRanges::iterator;
    using ConstNonDeletedRangesIt = NonDeletedRanges::const_iterator;

    TombstoneRanges() = delete;
    explicit TombstoneRanges(const Tombstones& tombstones);

    void populateRanges(const std::vector<EdgeRecord>& unfilteredContainer);

    template <TypedInternalID IDT>
    void populateRanges(const std::vector<EntityID>& unfilteredContainer);

    const NonDeletedRanges& getRanges() const { return _nonDeletedRanges; };
    size_t getNumRemainingEntities() const { return _numRemainingEntities; };

    ConstNonDeletedRangesIt cbegin() const { return _nonDeletedRanges.cbegin(); };
    ConstNonDeletedRangesIt cend() const { return _nonDeletedRanges.cend(); };

    NonDeletedRangesIt begin() { return _nonDeletedRanges.begin(); };
    NonDeletedRangesIt end() { return _nonDeletedRanges.end(); };

    ConstNonDeletedRangesIt begin() const { return _nonDeletedRanges.begin(); }
    ConstNonDeletedRangesIt end() const { return _nonDeletedRanges.end(); }

private:
    NonDeletedRanges _nonDeletedRanges;
    size_t _numRemainingEntities {0};
    const Tombstones& _tombstones;
};

}
