#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class CommitNode : public PlanGraphNode {
public:
    explicit CommitNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::COMMIT)
    {
    }
};

}
