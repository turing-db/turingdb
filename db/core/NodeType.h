// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <set>

#include "DBEntityType.h"

#include "Range.h"

namespace db {

class EdgeType;
class DB;
class Writeback;

class NodeType : public DBEntityType {
public:
    friend DB;
    friend Writeback;
    using NodeTypes = std::set<EdgeType*, DBObject::Comparator>;
    using NodeTypeRange = STLRange<NodeTypes>;

    NodeTypeRange inEdgeTypes() const;
    NodeTypeRange outEdgeTypes() const;

private:
    NodeTypes _inEdgeTypes;
    NodeTypes _outEdgeTypes;

    NodeType(DBIndex index, StringRef name);
    ~NodeType();
};

}
