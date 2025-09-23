#pragma once

#include "nodes/PlanGraphNode.h"
namespace db::v2 {

class CartesianProductsNode : public PlanGraphNode {
public:
    CartesianProductsNode()
        : PlanGraphNode(PlanGraphOpcode::CARTESIAN_PRODUCT)
    {
    }
};

}
