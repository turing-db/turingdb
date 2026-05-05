#pragma once

#include "ArcManager.h"

#include "HAMTIndexNode.h"

namespace db {

class HAMTManager {
public:
    template <typename K, typename V>
    WeakArc<HAMTIndexNode> newLeaf() {
        HAMTLeaf<K, V>* leaf = new HAMTLeaf<K, V>();
        WeakArc<HAMTIndexNode> rc = _nodes.takeOwnership(leaf);
        return rc;
    }

    WeakArc<HAMTIndexNode> newInner() {
        HAMTInnerNode* inner = new HAMTInnerNode();
        WeakArc<HAMTIndexNode> rc = _nodes.takeOwnership(inner);
        return rc;
    }

private:
    ArcManager<HAMTIndexNode> _nodes;
};

}
