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
    for (Edge* edge : _edges) {
        if (edge->getSource()->getNetwork() == this) {
            delete edge;
        }
    }
    _edges.clear();

    for (Node* node : _nodes) {
        delete node;
    }
    _nodes.clear();
}

size_t Network::getNodeCount() const {
    return _nodes.size();
}

Node* Network::getNode(DBIndex id) const {
    bioassert(_nodes.size() > id);
    return _nodes[id];
}

Network::NodeRange Network::nodes() const {
    return NodeRange(&_nodes);
}

Network::EdgeRange Network::edges() const {
    return EdgeRange(&_edges);
}

void Network::addNode(Node* node) {
    _nodes.push_back(node);
}

void Network::addEdge(Edge* edge) {
    _edges.push_back(edge);
}
