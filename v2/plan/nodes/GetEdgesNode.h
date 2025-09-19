#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class GetEdgesNode : public PlanGraphNode {
public:
    explicit GetEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_EDGES)
    {
    }
};

}
