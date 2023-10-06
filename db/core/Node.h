// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <set>

#include "DBEntity.h"

#include "StringRef.h"
#include "EdgeMap.h"
#include "Comparator.h"

namespace db {

class DB;
class NodeType;
class Writeback;
class Network;

class Node : public DBEntity {
public:
    friend DB;
    friend Writeback;
    friend DBComparator;
    using EdgeMapRange = EdgeMap::EdgeRange;
    using EdgeRange = EdgeMap::EdgeVectorRange;
    using Set = std::set<Node*, DBObject::Sorter>;

    NodeType* getType() const { return (NodeType*)DBEntity::getType(); }

    Network* getNetwork() const { return _net; }

    bool isAnonymous() const { return _name.empty(); }
    StringRef getName() const { return _name; }
    void setName(StringRef name);

    EdgeMapRange inEdges() const;
    EdgeRange inEdges(const EdgeType* type) const;
    size_t inEdgeCount() const { return _inEdgeCount; }

    EdgeMapRange outEdges() const;
    EdgeRange outEdges(const EdgeType* type) const;
    size_t outEdgeCount() const { return _outEdgeCount; }

private:
    Network* _net {nullptr};
    StringRef _name;
    EdgeMap _inEdges;
    EdgeMap _outEdges;
    size_t _inEdgeCount {0};
    size_t _outEdgeCount {0};

    Node(DBIndex index, NodeType* type, Network* net);
    ~Node();
    void addInEdge(Edge* edge);
    void addOutEdge(Edge* edge);
};

}

