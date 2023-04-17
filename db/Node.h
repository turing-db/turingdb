// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <unordered_map>

#include "StringRef.h"
#include "Value.h"

namespace db {

class NodeType;
class NodeDescriptor;
class Writeback;

class Node {
public:
    friend Writeback;

private:
    NodeDescriptor* _desc {nullptr};
    std::unordered_map<StringRef, Value> _properties;

    Node(NodeDescriptor* desc);
    ~Node();
};

}
