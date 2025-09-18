#pragma once

#include "PlanGraph.h"

namespace db::v2 {

class GetPropertyNode : public PlanGraphNode {
public:
    GetPropertyNode()
        : PlanGraphNode(PlanGraphOpcode::GET_PROPERTY)
    {
    }
};

}
