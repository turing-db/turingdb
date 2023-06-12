#pragma once

#include <vector>

#include "StringRef.h"
#include "ValueType.h"

namespace db {

class DB;
class Network;
class Node;
class NodeType;
class PropertyType;
class Edge;
class EdgeType;
class Property;
class DBEntityType;
class DBEntity;

class Writeback {
public:
    using NodeTypes = std::vector<NodeType*>;

    Writeback(DB* db);
    ~Writeback();

    Network* createNetwork(StringRef name);

    // Nodes
    Node* createNode(Network* net, NodeType* type);
    Node* createNode(Network* net, NodeType* type, StringRef name);

    // Edges
    Edge* createEdge(EdgeType* type, Node* source, Node* target);

    // Types
    NodeType* createNodeType(StringRef name);
    EdgeType* createEdgeType(StringRef name,
                             NodeType* source,
                             NodeType* target);
    EdgeType* createEdgeType(StringRef name,
                             const NodeTypes& sources,
                             const NodeTypes& targets);
    PropertyType* addPropertyType(NodeType* nodeType,
                                  StringRef name,
                                  ValueType type);
    PropertyType* addPropertyType(EdgeType* nodeType,
                                  StringRef name,
                                  ValueType type);

    bool addSourceNodeType(EdgeType* edgeType, NodeType* source);
    bool addTargetNodeType(EdgeType* edgeType, NodeType* target);

    // Properties
    bool setProperty(DBEntity* entity, const Property& prop);

private:
    DB* _db {nullptr};

    PropertyType* addPropertyTypeBase(DBEntityType* dbType,
                                      StringRef name,
                                      ValueType type);
};

}
