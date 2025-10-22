#pragma once

#include <queue>
#include <unordered_set>

namespace db::v2 {

class VarNode;
class PlanGraphNode;

class PlanGraphTopology {
public:
    PlanGraphTopology();
    ~PlanGraphTopology();

    enum class PathToDependency {
        SameVar,
        BackwardPath,
        UndirectedPath,
        NoPath
    };

    PathToDependency getShortestPath(PlanGraphNode* origin, PlanGraphNode* target);
    PlanGraphNode* getBranchTip(PlanGraphNode* origin);
    bool detectLoopsFrom(PlanGraphNode* origin);
    PlanGraphNode* findCommonSuccessor(PlanGraphNode* a, PlanGraphNode* b);
    VarNode* findNextVar(PlanGraphNode* node);

private:
    /// Containers used by the algorithms
    std::unordered_set<PlanGraphNode*> _visited;
    std::queue<PlanGraphNode*> _q1;
    std::queue<PlanGraphNode*> _q2;
};

}
