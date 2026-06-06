#pragma once

#include <queue>
#include <unordered_map>

#include "ID.h"

namespace db {

template <typename T>
struct DijkstraNode {
    NodeID id;
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
struct DijkstraNodeComparator {
    bool operator()(const DijkstraNode<T> l, const DijkstraNode<T> r) const {
        return l.distance > r.distance;
    }
};

template <typename T>
struct DijkstraHeapValues {
    NodeID prevNode;
    EdgeID edge;
    T distance {0};
};

template <typename T>
using DijkstraHeap = std::priority_queue<DijkstraNode<T>,
                                         std::vector<DijkstraNode<T>>,
                                         DijkstraNodeComparator<T>>;

template <typename T>
using DijkstraValueMap = std::unordered_map<NodeID, DijkstraHeapValues<T>>;

}
