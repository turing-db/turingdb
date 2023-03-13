// Copyright 2023 Turing Biosystems Ltd.

#include "Network.h"

#include "DB.h"
#include "Node.h"

using namespace db;

Network::Network(StringRef displayName)
    : _displayName(displayName)
{
}

Network::~Network() {
    for (Network* net : _subNets) {
        delete net;
    }

    for (Node* node : _nodes) {
        delete node;
    }
}

Network* Network::create(DB* db, StringRef displayName) {
    Network* net = new Network(displayName);
    db->addNetwork(net);
    return net;
}

Network* Network::create(DB* db, Network* parent, StringRef displayName) {
    Network* net = create(db, displayName);
    net->setParent(parent);
    parent->addSubNet(net);
    return net;
}

void Network::setParent(Network* parent) {
    _parent = parent;
}

void Network::addSubNet(Network* subNet) {
    _subNets.push_back(subNet);
}

void Network::addNode(Node* node) {
    _nodes.push_back(node);
}
