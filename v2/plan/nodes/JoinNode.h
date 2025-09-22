#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class JoinNode : public PlanGraphNode {
public:
    JoinNode()
        : PlanGraphNode(PlanGraphOpcode::JOIN)
    {
    }
};

}
