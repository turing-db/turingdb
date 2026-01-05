#pragma once

#include "PlanGraphNode.h"

namespace db::v2 {

class CreateEdgeNode : public PlanGraphNode {
public:
    explicit CreateEdgeNode()
        : PlanGraphNode(PlanGraphOpcode::CREATE_EDGE)
    {
    }
};

}
