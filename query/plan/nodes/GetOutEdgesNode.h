#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class GetOutEdgesNode : public PlanGraphNode {
public:
    explicit GetOutEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_OUT_EDGES)
    {
    }
};

}
