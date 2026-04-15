#pragma once

#include <vector>

#include <cstdint>

#include "ArcManager.h"

namespace db {

template <typename K, typename V>
class HAMTNode {
public:
    using ChildBitmask = uint32_t;
    using Children = std::vector<WeakArc<HAMTNode>>;

    ChildBitmask mask() const { return _mask; }
    const Children& children() const { return _children; }

private:
    ChildBitmask _mask {0};
    Children _children;
};

}

