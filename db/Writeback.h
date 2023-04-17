#pragma once

#include "StringRef.h"

namespace db {

class DB;
class Network;
class Node;
class NodeType;
class ComponentType;

class Writeback {
public:
    Writeback(DB* db);
    ~Writeback();

    Network* createNetwork(StringRef name);

    Node* createNode(Network* net, NodeType* type);

    NodeType* createNodeType(StringRef name);
    ComponentType* createComponentType(StringRef name);

private:
    DB* _db {nullptr};
};

}
