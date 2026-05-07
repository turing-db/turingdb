#pragma once

#include <unordered_map>

#include "VisitedNodeSet.h"

namespace db {

class VarNode;
class PlanGraphNode;
class PlanGraph;

class PlanGraphTopology {
public:
    PlanGraphTopology(const PlanGraph* tree);
    ~PlanGraphTopology();

    enum class PathToDependency {
        SameVar,
        BackwardPath,
        UndirectedPath,
        NoPath
    };

    using PathInfo = std::pair<PlanGraphTopology::PathToDependency, PlanGraphNode*>;

    PathInfo getShortestPath(PlanGraphNode* origin, PlanGraphNode* target);
    PlanGraphNode* getBranchTip(PlanGraphNode* origin);
    bool detectLoopsFrom(PlanGraphNode* origin);
    PlanGraphNode* findCommonSuccessor(PlanGraphNode* a, PlanGraphNode* b);
    VarNode* findNextVar(PlanGraphNode* node);

private:
    /// Visited set used by the algorithms. Clear at the beginning of each algorithm
    VisitedNodeSet _visited;
    const PlanGraph* _tree {nullptr};

    /// Cache of the common successors
    struct NodePair {
        PlanGraphNode* _a {nullptr};
        PlanGraphNode* _b {nullptr};

        struct Hasher {
            std::size_t operator()(const NodePair& pair) const {
                return std::hash<PlanGraphNode*>()(pair._a)
                     ^ std::hash<PlanGraphNode*>()(pair._b);
            }
        };

        struct Equal {
            bool operator()(const NodePair& a, const NodePair& b) const {
                return a._a == b._a && a._b == b._b;
            }
        };
    };

    using CommonSuccessorCache = std::unordered_map<NodePair, PlanGraphNode*, NodePair::Hasher, NodePair::Equal>;
    CommonSuccessorCache _commonSuccessors;
};

}
