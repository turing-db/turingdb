#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ListGraphNode : public PlanGraphNode {
public:
    explicit ListGraphNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::LIST_GRAPH)
    {
    }
};

}
