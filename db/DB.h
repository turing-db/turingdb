// Copyright 2023 Turing Biosystems Ltd.

#ifndef _DB_DB_
#define _DB_DB_

#include <vector>

#include "StringIndex.h"

namespace db {

class ValueType;
class Network;
class Edge;
class NodeType;
class EdgeType;

class DB {
public:
    friend Network;
    friend Edge;
    friend NodeType;
    friend EdgeType;
    using Networks = std::vector<Network*>;
    using Edges = std::vector<Edge*>;

    ~DB();

    static DB* create();

    StringRef getString(const std::string& str);

    // Value types
    ValueType* getInt() const { return _int; }
    ValueType* getUnsigned() const { return _unsigned; }
    ValueType* getBool() const { return _bool; }
    ValueType* getDecimal() const { return _decimal; }
    ValueType* getString() const { return _string; }

private:
    using NodeTypes = std::vector<NodeType*>;
    using EdgeTypes = std::vector<EdgeType*>;

    StringIndex _strIndex;
    Networks _networks;
    Edges _edges;
    NodeTypes _nodeTypes;
    EdgeTypes _edgeTypes;

    ValueType* _int {nullptr};
    ValueType* _unsigned {nullptr};
    ValueType* _bool {nullptr};
    ValueType* _decimal {nullptr};
    ValueType* _string {nullptr};

    DB();
    void addNetwork(Network* net);
    void addEdge(Edge* edge);
    void addNodeType(NodeType* nodeType);
    void addEdgeType(EdgeType* edgeType);
};

}

#endif
