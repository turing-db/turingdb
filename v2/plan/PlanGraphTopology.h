#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class PlanGraphTopology {
public:
    enum class PathToDependency {
        SameVar,
        BackwardPath,
        UndirectedPath,
        NoPath
    };

    static PathToDependency getShortestPath(const PlanGraphNode* origin, const PlanGraphNode* target);
    static PlanGraphNode* getBranchTip(PlanGraphNode* origin);
};

}
