#pragma once

#include "ArcManager.h"

#include "HAMTNode.h"

namespace db {

template <typename K, typename V>
class HAMTManager {
public:

    WeakArc<HAMTNode<K, V>> newNode() { return _nodes.create(); }

private:
    ArcManager<HAMTNode<K,V>> _nodes;
};

}
