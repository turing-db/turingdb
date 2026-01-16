#pragma once

#include "nodes/PlanGraphNode.h"

namespace db {

class ShowProceduresNode : public PlanGraphNode {
public:
    ShowProceduresNode()
        : PlanGraphNode(PlanGraphOpcode::SHOW_PROCEDURES)
    {
    }
};

}
