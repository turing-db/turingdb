#pragma once

#include "LabelID.h"

namespace db {

class Node;

class NodeAccessor {
public:
    NodeAccessor(Node* node)
        : _node(node)
    {
    }

private:
    Node* _node {nullptr};
};

}