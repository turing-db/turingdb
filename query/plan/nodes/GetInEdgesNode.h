#pragma once

#include "PlanGraphNode.h"

namespace db {

class GetInEdgesNode : public PlanGraphNode {
public:
    explicit GetInEdgesNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::GET_IN_EDGES)
    {
    }
};

}
