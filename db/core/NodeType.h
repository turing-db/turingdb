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
    using EdgeTypes = std::set<EdgeType*, DBObject::Comparator>;
    using EdgeTypeRange = STLRange<EdgeTypes>;

    EdgeTypeRange inEdgeTypes() const;
    EdgeTypeRange outEdgeTypes() const;

private:
    EdgeTypes _inEdgeTypes;
    EdgeTypes _outEdgeTypes;

    NodeType(DBIndex index, StringRef name);
    ~NodeType();
};

}
