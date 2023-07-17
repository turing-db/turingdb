// Copyright 2023 Turing Biosystems Ltd.

#include "Network.h"

#include "BioAssert.h"
#include "Node.h"
#include "Edge.h"

using namespace db;

Network::Network(DBIndex index, StringRef name)
    : DBObject(index),
    _name(name)
{
}

Network::~Network() {
    _edges.clear();
    _nodes.clear();
}

size_t Network::getNodeCount() const {
    return _nodes.size();
}

Node* Network::getNode(DBIndex id) const {
    const auto foundIt = _nodes.find(id);
    if (foundIt == _nodes.end()) {
        return nullptr;
    }
    return foundIt->second;
}

Edge* Network::getEdge(DBIndex id) const {
    const auto foundIt = _edges.find(id);
    if (foundIt == _edges.end()) {
        return nullptr;
    }
    return foundIt->second;
}

Network::NodeRange Network::nodes() const {
    return NodeRange(&_nodes);
}

Network::EdgeRange Network::edges() const {
    return EdgeRange(&_edges);
}

void Network::addNode(Node* node) {
    _nodes[node->getIndex()] = node;
}

void Network::addEdge(Edge* edge) {
    _edges[edge->getIndex()] = edge;
}
