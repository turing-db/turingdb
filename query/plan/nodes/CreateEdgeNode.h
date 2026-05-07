#pragma once

#include "PlanGraphNode.h"

namespace db {

class CreateEdgeNode : public PlanGraphNode {
public:
    explicit CreateEdgeNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::CREATE_EDGE)
    {
    }
};

}
