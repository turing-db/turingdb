#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class CommitNode : public PlanGraphNode {
public:
    CommitNode()
        : PlanGraphNode(PlanGraphOpcode::COMMIT)
    {
    }
};

}
