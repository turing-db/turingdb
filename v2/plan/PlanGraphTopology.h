#pragma once

#include "nodes/PlanGraphNode.h"

namespace db::v2 {

class VarNode;

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
    static bool detectLoops(const PlanGraphNode* origin);
    static const PlanGraphNode* findCommonSuccessor(const PlanGraphNode* a, const PlanGraphNode* b);
    static const VarNode* findNextVar(const PlanGraphNode* node);
};

}
