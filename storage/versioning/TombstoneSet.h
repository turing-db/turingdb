#pragma once

#include <unordered_set>

#include "ID.h"

namespace db {

template <TypedInternalID IDT>
class TombstoneSet {
public:
    bool contains(IDT id);
    auto insert(IDT id);

private:
    std::unordered_set<IDT> _set;
};

template <TypedInternalID IDT>
bool TombstoneSet<IDT>::contains(IDT id) {
    return _set.contains(id);
}

template <TypedInternalID IDT>
auto TombstoneSet<IDT>::insert(IDT id) {
    return _set.insert(id);
}

}
