// Copyright 2023 Turing Biosystems Ltd.

#include "Node.h"

#include "Network.h"

using namespace db;

Node::Node(Network* net, NodeType* type)
    : _type(type), _net(net)
{
}

Node::~Node() {
}

Node* Node::create(Network* net, NodeType* type) {
    Node* obj = new Node(net, type);
    net->addNode(obj);
    return obj;
}
