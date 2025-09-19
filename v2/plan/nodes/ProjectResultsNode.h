#pragma once

#include "nodes/PlanGraphNode.h"
namespace db::v2 {

class ProjectResultsNode : public PlanGraphNode {
public:
    ProjectResultsNode()
        : PlanGraphNode(PlanGraphOpcode::PROJECT_RESULTS)
    {
    }
};

}
