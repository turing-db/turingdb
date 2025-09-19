#pragma once

#include <unordered_map>

namespace db::v2 {

class VarDecl;
class VarNode;
class FilterNode;
class PlanGraphNode;
class PlanGraph;

class PlanGraphVariables {
public:
    explicit PlanGraphVariables(PlanGraph& tree);
    ~PlanGraphVariables();

    PlanGraphVariables(const PlanGraphVariables&) = delete;
    PlanGraphVariables(PlanGraphVariables&&) = delete;
    PlanGraphVariables& operator=(const PlanGraphVariables&) = delete;
    PlanGraphVariables& operator=(PlanGraphVariables&&) = delete;

    VarNode* getVarNode(const VarDecl* varDecl) const;
    FilterNode* getNodeFilter(const VarNode* varNode) const;

    std::tuple<VarNode*, FilterNode*> getOrCreateVarNodeAndFilter(const VarDecl* varDecl);

private:
    PlanGraph* _tree {nullptr};

    // Map of variable nodes
    std::unordered_map<const VarDecl*, VarNode*> _varNodesMap;

    // Map of node filters
    std::unordered_map<const VarNode*, FilterNode*> _nodeFiltersMap;
};

}
