#pragma once

#include <unordered_map>
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
    /// Visited set used by the algorithms. Clear at the beginning of each algorithm
    std::unordered_set<PlanGraphNode*> _visited;

    /// Cache of the common successors
    struct NodePair {
        PlanGraphNode* a {nullptr};
        PlanGraphNode* b {nullptr};

        struct Hasher {
            std::size_t operator()(const NodePair& pair) const {
                return std::hash<PlanGraphNode*>()(pair.a)
                     ^ std::hash<PlanGraphNode*>()(pair.b);
            }
        };

        struct Equal {
            bool operator()(const NodePair& a, const NodePair& b) const {
                return a.a == b.a && a.b == b.b;
            }
        };
    };

    using CommonSuccessorCache = std::unordered_map<NodePair, PlanGraphNode*, NodePair::Hasher, NodePair::Equal>;
    CommonSuccessorCache _commonSuccessors;
};

}
