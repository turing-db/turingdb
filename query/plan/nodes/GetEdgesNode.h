#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetEdgesNode : public PlanGraphNode {
public:
    explicit GetEdgesNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::GET_EDGES)
    {
    }
};

}
