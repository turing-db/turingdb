#pragma once

#include "ID.h"

namespace db {

template <TypedInternalID IDT>
class WriteSet;

template <TypedInternalID IDT>
class WriteSetComparator {
public:
    [[nodiscard]] static bool same(const WriteSet<IDT>& setA, const WriteSet<IDT>& setB);
};

}
