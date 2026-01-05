#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class CommitNode : public PlanGraphNode {
public:
    CommitNode()
        : PlanGraphNode(PlanGraphOpcode::COMMIT)
    {
    }
};

}
