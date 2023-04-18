// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <unordered_map>
#include <vector>

#include "StringRef.h"
#include "Value.h"

namespace db {

class NodeType;
class NodeDescriptor;
class Property;
class Edge;
class EdgeType;
class Writeback;

class Node {
public:
    friend Writeback;

    Value getProperty(const Property* prop) const;

private:
    using EdgeVector = std::vector<Edge*>;

    NodeDescriptor* _desc {nullptr};
    std::unordered_map<StringRef, Value> _properties;
    std::unordered_map<EdgeType*, EdgeVector> _inEdges;
    std::unordered_map<EdgeType*, EdgeVector> _outEdges;

    Node(NodeDescriptor* desc);
    ~Node();
    void setDescriptor(NodeDescriptor* desc);
    void addInEdge(Edge* edge);
    void addOutEdge(Edge* edge);
};

}
