#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetOutEdgesNode : public PlanGraphNode {
public:
    explicit GetOutEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_OUT_EDGES)
    {
    }
};

}
