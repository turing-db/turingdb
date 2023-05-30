// Copyright 2023 Turing Biosystems Ltd.

#include "NodeType.h"

using namespace db;

NodeType::NodeType(DBIndex index, StringRef name)
    : DBEntityType(index, name)
{
}

NodeType::~NodeType() {
}

NodeType::EdgeTypeRange NodeType::inEdgeTypes() const {
    return EdgeTypeRange(&_inEdgeTypes);
}

NodeType::EdgeTypeRange NodeType::outEdgeTypes() const {
    return EdgeTypeRange(&_outEdgeTypes);
}
