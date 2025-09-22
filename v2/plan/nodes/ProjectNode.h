#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class ProjectNode : public PlanGraphNode {
public:
    ProjectNode()
        : PlanGraphNode(PlanGraphOpcode::PROJECT)
    {
    }
};

}
