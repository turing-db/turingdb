#include "HAMTIndexNode.h"

using namespace db;

void HAMTInnerNode::insertChild(size_t hashChunk, WeakArc<HAMTIndexNode>& child) {
    const ChildBitmask childBitString = 1U << hashChunk;
    const ChildBitmask chldrnBelowMask = childBitString - 1;
    const ChildBitmask m = _mask & chldrnBelowMask;
    const size_t index = std::popcount(m);
    _children.insert(_children.begin() + index, child);
    _mask |= childBitString;
}
