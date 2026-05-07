#pragma once

#include "nodes/PlanGraphNode.h"
namespace db {

class CartesianProductNode : public PlanGraphNode {
public:
    explicit CartesianProductNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::CARTESIAN_PRODUCT)
    {
    }
};

}
