// Copyright 2023 Turing Biosystems Ltd.

#include "Network.h"

using namespace db;

Network::Network(StringRef name)
    : _name(name)
{
}

Network::~Network() {
}

void Network::addNode(Node* node) {
    _nodes.push_back(node);
}
