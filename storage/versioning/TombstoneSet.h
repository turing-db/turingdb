#pragma once

#include <unordered_set>

#include "ID.h"

namespace db {

template <TypedInternalID IDT>
class TombstoneSet {
public:
private:
    std::unordered_set<IDT> _set;
};

}
