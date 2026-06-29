#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ListAvailableGraphsNode : public PlanGraphNode {
public:
    explicit ListAvailableGraphsNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::LIST_AVAILABLE_GRAPHS)
    {
    }
};

}
