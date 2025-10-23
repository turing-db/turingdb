#pragma once

#include "ID.h"

namespace db {

template <TypedInternalID IDT>
class TombstoneSet;

template <TypedInternalID IDT>
class TombstoneSetComparator {
public:
    [[nodiscard]] static bool same(const TombstoneSet<IDT>& setA,
                                   const TombstoneSet<IDT>& setB);
};

}
