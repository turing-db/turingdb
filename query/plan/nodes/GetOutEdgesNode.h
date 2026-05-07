#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetOutEdgesNode : public PlanGraphNode {
public:
    explicit GetOutEdgesNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::GET_OUT_EDGES)
    {
    }
};

}
