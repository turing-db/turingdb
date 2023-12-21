#pragma once

#include "EntityID.h"
#include "RWSpinLock.h"

namespace db {

class NodeAccessor;

class Node {
public:
    friend NodeAccessor;

private:
    EntityID _id;
    bool _deleted {false};
    mutable RWSpinLock _lock;

    Node(EntityID id);
    ~Node();
};

}