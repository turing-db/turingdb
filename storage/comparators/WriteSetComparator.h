#pragma once

#include "ID.h"

namespace db {

template <typename IDT>
class WriteSet;

template <typename IDT>
class WriteSetComparator {
public:
    [[nodiscard]] static bool same(const WriteSet<IDT>& setA, const WriteSet<IDT>& setB);
};

}
