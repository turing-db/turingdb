#pragma once

#include "nodes/PlanGraphNode.h"
namespace db::v2 {

class CartesianProductNode : public PlanGraphNode {
public:
    CartesianProductNode()
        : PlanGraphNode(PlanGraphOpcode::CARTESIAN_PRODUCT)
    {
    }
};

}
