#pragma once

#include "ID.h"
#include "versioning/WriteSet.h"

namespace db {

template <TypedInternalID IDT>
class WriteSetComparator{
public:
    [[nodiscard]] static bool same(WriteSet<IDT> setA, WriteSet<IDT> setB);
};

}
