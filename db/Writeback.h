#pragma once

#include "StringRef.h"

namespace db {

class DB;
class Network;
class Node;
class NodeType;
class ComponentType;
class ValueType;
class Property;
class Edge;
class EdgeType;

class Writeback {
public:
    Writeback(DB* db);
    ~Writeback();

    Network* createNetwork(StringRef name);

    // Nodes
    Node* createNode(Network* net, NodeType* type);
    bool addComponent(Node* node, ComponentType* compType);

    // Edges
    Edge* createEdge(EdgeType* type, Node* source, Node* target);

    // Types and components
    NodeType* createNodeType(StringRef name);
    EdgeType* createEdgeType(StringRef name,
                             ComponentType* sourceComp,
                             ComponentType* targetComp);
    ComponentType* createComponentType(StringRef name);

    // Properties
    Property* addProperty(ComponentType* compType,
                          StringRef name,
                          ValueType* valType);

private:
    DB* _db {nullptr};
};

}
