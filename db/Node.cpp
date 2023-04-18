// Copyright 2023 Turing Biosystems Ltd.

#include "Node.h"

#include "NodeDescriptor.h"
#include "Property.h"
#include "Edge.h"

using namespace db;

Node::Node(NodeDescriptor* desc)
    : _desc(desc)
{
}

Node::~Node() {
}

Value Node::getProperty(const Property* prop) const {
    if (!_desc->hasComponent(prop->getComponentType())) {
        return Value();
    }

    const auto it = _properties.find(prop->getName());
    if (it == _properties.end()) {
        return Value();
    }

    return it->second;
}

void Node::setDescriptor(NodeDescriptor* desc) {
    _desc = desc;
}

void Node::addInEdge(Edge* edge) {
    _inEdges[edge->getType()].push_back(edge);
}

void Node::addOutEdge(Edge* edge) {
    _outEdges[edge->getType()].push_back(edge);
}
