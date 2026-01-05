#pragma once

#include "nodes/PlanGraphNode.h"
namespace db {

class CartesianProductNode : public PlanGraphNode {
public:
    CartesianProductNode()
        : PlanGraphNode(PlanGraphOpcode::CARTESIAN_PRODUCT)
    {
    }
};

}
