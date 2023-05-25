// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include "DBEntity.h"

namespace db {

class Node;
class EdgeType;
class Writeback;

class Edge : public DBEntity {
public:
    friend Writeback;

    EdgeType* getType() const { return (EdgeType*)DBEntity::getType(); }
    Node* getSource() const { return _source; }
    Node* getTarget() const { return _target; }

private:
    Node* _source {nullptr};
    Node* _target {nullptr};

    Edge(DBIndex index, EdgeType* type, Node* source, Node* target);
    ~Edge();
};

}
