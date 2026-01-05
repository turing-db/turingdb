#pragma once

#include "PlanGraphNode.h"

namespace db {

class CreateEdgeNode : public PlanGraphNode {
public:
    explicit CreateEdgeNode()
        : PlanGraphNode(PlanGraphOpcode::CREATE_EDGE)
    {
    }
};

}
