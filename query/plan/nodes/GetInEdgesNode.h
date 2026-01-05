#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class GetInEdgesNode : public PlanGraphNode {
public:
    explicit GetInEdgesNode()
        : PlanGraphNode(PlanGraphOpcode::GET_IN_EDGES)
    {
    }
};

}
