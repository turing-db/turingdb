#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetEdgeTargetNode : public PlanGraphNode {
public:
    explicit GetEdgeTargetNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGE_TARGET)
    {
    }
};

}
