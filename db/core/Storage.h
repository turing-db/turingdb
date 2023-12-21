#pragma once

#include <map>
#include <mutex>
#include <shared_mutex>

#include "EntityID.h"
#include "StorageAccessor.h"

namespace db {

class Node;
class Edge;

class Storage {
public:
    friend StorageAccessor;

    Storage();
    ~Storage();

    Storage(const Storage&) = delete;
    Storage(Storage&&) = delete;
    Storage& operator=(const Storage&) = delete;
    Storage& operator=(Storage&&) = delete;

    StorageAccessor access();

private:
    mutable std::shared_mutex _mainLock;

    std::map<EntityID, Node*> _nodes;
    std::map<EntityID, Edge*> _edges;
};

}