#pragma once

#include <utility>

#include "ArcManager.h"

#include "HAMTIndexNode.h"

namespace db {

class HAMTManager {
public:
    WeakArc<HAMTIndexNode> newLeaf() {
        return _nodes.create(std::in_place_type<HAMTLeaf>);
    }
    WeakArc<HAMTIndexNode> newInner() {
        return _nodes.create(std::in_place_type<HAMTInnerNode>);
    }

private:
    ArcManager<HAMTIndexNode> _nodes;
};

}
