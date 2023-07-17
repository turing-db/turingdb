// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <map>

#include "DBObject.h"
#include "StringRef.h"
#include "Range.h"
#include "Comparator.h"

namespace db {

class Node;
class Edge;
class DB;
class Writeback;

class Network : public DBObject {
public:
    friend DB;
    friend Writeback;
    friend DBComparator;
    using Nodes = std::map<DBIndex, Node*>;
    using Edges = std::map<DBIndex, Edge*>;
    using NodeRange = STLRange<Nodes>;
    using EdgeRange = STLRange<Edges>;

    StringRef getName() const { return _name; }
    size_t getNodeCount() const;

    Node* getNode(DBIndex id) const;
    Edge* getEdge(DBIndex id) const;

    NodeRange nodes() const;
    EdgeRange edges() const;

private:
    StringRef _name;
    Nodes _nodes;
    Edges _edges;

    Network(DBIndex index, StringRef name);
    ~Network();
    void addNode(Node* node);
    void addEdge(Edge* edge);
};

}
