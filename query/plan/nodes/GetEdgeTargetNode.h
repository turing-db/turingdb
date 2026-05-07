#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetEdgeTargetNode : public PlanGraphNode {
public:
    explicit GetEdgeTargetNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::GET_EDGE_TARGET)
    {
    }
};

}
