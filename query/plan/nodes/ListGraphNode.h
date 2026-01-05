#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ListGraphNode : public PlanGraphNode {
public:
    ListGraphNode()
        : PlanGraphNode(PlanGraphOpcode::LIST_GRAPH)
    {
    }
};

}
