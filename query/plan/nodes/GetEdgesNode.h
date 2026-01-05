#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetEdgesNode : public PlanGraphNode {
public:
    explicit GetEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGES)
    {
    }
};

}
