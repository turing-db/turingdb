#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class ListGraphNode : public PlanGraphNode {
public:
    ListGraphNode()
        : PlanGraphNode(PlanGraphOpcode::LIST_GRAPH)
    {
    }
};

}
