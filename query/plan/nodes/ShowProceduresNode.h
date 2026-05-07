#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ShowProceduresNode : public PlanGraphNode {
public:
    explicit ShowProceduresNode(PlanGraphNodeID id)
        : PlanGraphNode(id, PlanGraphOpcode::SHOW_PROCEDURES)
    {
    }
};

}
