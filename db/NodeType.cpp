// Copyright 2023 Turing Biosystems Ltd.

#include "NodeType.h"

#include "DB.h"
#include "EdgeType.h"

using namespace db;

NodeType::NodeType(StringRef name)
    : _name(name)
{
}

NodeType::~NodeType() {
}

NodeType* NodeType::create(DB* db, StringRef name) {
    NodeType* nodeType = new NodeType(name);
    db->addNodeType(nodeType);
    return nodeType;
}

void NodeType::addInEdgeType(EdgeType* edgeType) {
    _inEdgeTypes.push_back(edgeType);
}

void NodeType::addOutEdgeType(EdgeType* edgeType) {
    _outEdgeTypes.push_back(edgeType);
}
