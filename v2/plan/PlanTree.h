#pragma once

#include "PlanTreeNode.h"

#include "Arena.h"

namespace db {

class PlanTree {
public:
    PlanTree();
    ~PlanTree();

    PlanTreeNode* create(PlanTreeOpcode opcode) {
        return &_nodes.emplace_back(opcode);
    }

private:
    Arena<PlanTreeNode> _nodes;
};

}
