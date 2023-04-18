// Copyright 2023 Turing Biosystems Ltd.

#pragma once

namespace db {

class Node;
class EdgeType;
class Writeback;

class Edge {
public:
    friend Writeback;

    EdgeType* getType() const { return _type; }
    Node* getSource() const { return _source; }
    Node* getTarget() const { return _target; }

private:
    EdgeType* _type {nullptr};
    Node* _source {nullptr};
    Node* _target {nullptr};

    Edge(EdgeType* type, Node* source, Node* target);
    ~Edge();
};

}
