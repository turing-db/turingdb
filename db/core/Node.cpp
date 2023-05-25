// Copyright 2023 Turing Biosystems Ltd.

#include "Node.h"

#include "NodeType.h"
#include "Edge.h"
#include "EdgeType.h"

using namespace db;

Node::Node(DBIndex index, NodeType* type, Network* net)
    : DBEntity(index, type),
    _net(net)
{
}

Node::~Node() {
}

void Node::addInEdge(Edge* edge) {
    _inEdges[edge->getType()].push_back(edge);
}

void Node::addOutEdge(Edge* edge) {
    _outEdges[edge->getType()].push_back(edge);
}
