// Copyright 2023 Turing Biosystems Ltd.

#include "Node.h"

#include "NodeType.h"

using namespace db;

Node::Node(DBIndex index, NodeType* type, Network* net)
    : DBEntity(index, type),
    _net(net)
{
}

Node::~Node() {
}

Node::EdgeMapRange Node::inEdges() const {
    return _inEdges.edges();
}

Node::EdgeRange Node::inEdges(const EdgeType* type) const {
    return _inEdges.edges(type);
}

Node::EdgeMapRange Node::outEdges() const {
    return _outEdges.edges();
}

Node::EdgeRange Node::outEdges(const EdgeType* type) const {
    return _outEdges.edges(type);
}

void Node::setName(StringRef name) {
    _name = name;
}

void Node::addInEdge(Edge* edge) {
    _inEdges.addEdge(edge);
    _inEdgeCount++;
}

void Node::addOutEdge(Edge* edge) {
    _outEdges.addEdge(edge);
    _outEdgeCount++;
}

