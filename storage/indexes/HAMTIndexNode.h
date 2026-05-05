#pragma once

#include <variant>
#include <vector>

#include <stdint.h>

#include "ArcManager.h"

namespace db {

class Column;

class HAMTIndexNode {
public:
    enum class Kind : uint8_t {
        INNER,
        LEAF,
    };
};

class HAMTInnerNode final : public HAMTIndexNode{
public:
    using ChildBitmask = uint32_t;
    using Children = std::vector<WeakArc<HAMTInnerNode>>;

    ChildBitmask mask() const { return _mask; }
    const Children& children() const { return _children; }

private:
    constexpr static HAMTIndexNode::Kind _kind {HAMTIndexNode::Kind::INNER};

    ChildBitmask _mask {0};
    Children _children;
};

template <typename K, typename V>
class HAMTLeaf final : public HAMTIndexNode {
public:
    const Column* values() const { return _values; }

private:
    constexpr static HAMTIndexNode::Kind _kind {HAMTIndexNode::Kind::LEAF};

    Column* _values {nullptr};
};

}
