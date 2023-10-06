// Copyright 2023 Turing Biosystems Ltd.

#include "NodeType.h"
#include "EdgeType.h"

using namespace db;

NodeType::NodeType(DBIndex index, StringRef name)
    : DBEntityType(index, name)
{
}

NodeType::~NodeType() {
}

void NodeType::addInEdgeType(const db::EdgeType* et) {
    _inEdgeTypes.insert(et);
}

void NodeType::addOutEdgeType(const db::EdgeType* et) {
    _outEdgeTypes.insert(et);
}

NodeType::EdgeTypeRange NodeType::inEdgeTypes() const {
    return EdgeTypeRange(&_inEdgeTypes);
}

NodeType::EdgeTypeRange NodeType::outEdgeTypes() const {
    return EdgeTypeRange(&_outEdgeTypes);
}

