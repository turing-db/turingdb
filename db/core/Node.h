// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <map>
#include <vector>

#include "DBEntity.h"

#include "StringRef.h"

namespace db {

class Edge;
class Network;
class EdgeType;
class NodeType;
class Writeback;

class Node : public DBEntity {
public:
    friend Writeback;

    NodeType* getType() const { return (NodeType*)DBEntity::getType(); }

    Network* getNetwork() const { return _net; }

private:
    using EdgeVector = std::vector<Edge*>;

    Network* _net {nullptr};
    std::map<const EdgeType*, EdgeVector, DBObject::Comparator> _inEdges;
    std::map<const EdgeType*, EdgeVector, DBObject::Comparator> _outEdges;

    Node(DBIndex index, NodeType* type, Network* net);
    ~Node();
    void addInEdge(Edge* edge);
    void addOutEdge(Edge* edge);
};

}
