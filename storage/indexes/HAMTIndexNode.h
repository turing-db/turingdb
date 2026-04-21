#pragma once

#include <variant>
#include <vector>

#include <stdint.h>

#include "ArcManager.h"

namespace db {

class Column;

class HAMTInnerNode {
public:
    using ChildBitmask = uint32_t;
    using Children = std::vector<WeakArc<HAMTInnerNode>>;

    ChildBitmask mask() const { return _mask; }
    const Children& children() const { return _children; }

private:
    ChildBitmask _mask {0};
    Children _children;
};

class HAMTLeaf {
public:
    const Column* values() const { return _values; }

private:
    Column* _values {nullptr};
};

using HAMTIndexNode = std::variant<HAMTInnerNode, HAMTLeaf>;

}
