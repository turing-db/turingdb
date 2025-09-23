#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class MaterializeNode : public PlanGraphNode {
public:
    MaterializeNode()
        : PlanGraphNode(PlanGraphOpcode::MATERIALIZE)
    {
    }
};

}
