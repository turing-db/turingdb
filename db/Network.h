// Copyright 2023 Turing Biosystems Ltd.

#pragma once

#include <vector>

#include "StringRef.h"

namespace db {

class Node;
class Writeback;

class Network {
public:
    friend Writeback;

    StringRef getName() const { return _name; }

private:
    StringRef _name;
    std::vector<Node*> _nodes;

    Network(StringRef name);
    ~Network();
    void addNode(Node* node);
};

}
