// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <set>
#include <vector>

#include "DBEntityType.h"

#include "Range.h"

namespace db {

class NodeType;
class DB;
class Writeback;

class EdgeType : public DBEntityType {
public:
    friend DB;
    friend Writeback;
    using NodeTypes = std::set<NodeType*, DBObject::Comparator>;
    using NodeTypeRange = STLRange<NodeTypes>;

    bool hasSourceType(const NodeType* nodeType) const;
    bool hasTargetType(const NodeType* nodeType) const;

    NodeTypeRange sourceTypes() const;
    NodeTypeRange targetTypes() const;

private:
    NodeTypes _sourceTypes;
    NodeTypes _targetTypes;

    EdgeType(DBIndex index,
             StringRef name,
             const std::vector<NodeType*>& sources,
             const std::vector<NodeType*>& targets);
    ~EdgeType();

    void addSourceType(NodeType* nodeType);
    void addTargetType(NodeType* nodeType);
};

}
