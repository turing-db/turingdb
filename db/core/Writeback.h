#pragma once

#include <vector>

#include "StringRef.h"

namespace db {

class DB;
class Network;
class Node;
class NodeType;
class ValueType;
class PropertyType;
class Edge;
class EdgeType;
class Property;
class DBEntityType;

class Writeback {
public:
    using NodeTypes = std::vector<NodeType*>;

    Writeback(DB* db);
    ~Writeback();

    Network* createNetwork(StringRef name);

    // Nodes
    Node* createNode(Network* net, NodeType* type);

    // Edges
    Edge* createEdge(EdgeType* type, Node* source, Node* target);

    // Types
    NodeType* createNodeType(StringRef name);
    EdgeType* createEdgeType(StringRef name, NodeType* source, NodeType* target);
    EdgeType* createEdgeType(StringRef name,
                             const NodeTypes& sources,
                             const NodeTypes& targets);
    PropertyType* addPropertyType(NodeType* nodeType,
                                  StringRef name,
                                  ValueType* type);
    PropertyType* addPropertyType(EdgeType* nodeType,
                                  StringRef name,
                                  ValueType* type);

    // Properties
    bool addProperty(Node* node, const Property& prop);
    bool addProperty(Edge* edge, const Property& prop);

private:
    DB* _db {nullptr};

    PropertyType* addPropertyTypeBase(DBEntityType* dbType,
                                      StringRef name,
                                      ValueType* type);
};

}
