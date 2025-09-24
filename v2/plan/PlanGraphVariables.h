#pragma once

#include <cstdint>
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

    void resetDeclOrder() {
        _nextDeclOrder = 0;
    }

    VarNode* getVarNode(const VarDecl* varDecl) const;
    FilterNode* getNodeFilter(const VarNode* varNode) const;

    std::tuple<VarNode*, FilterNode*> getVarNodeAndFilter(const VarDecl* varDecl);
    std::tuple<VarNode*, FilterNode*> createVarNodeAndFilter(const VarDecl* varDecl);

    std::unordered_map<const VarDecl*, VarNode*>& getVarNodesMap() {
        return _varNodesMap;
    }

    std::unordered_map<const VarNode*, FilterNode*>& getNodeFiltersMap() {
        return _nodeFiltersMap;
    }

private:
    PlanGraph* _tree {nullptr};

    uint32_t _nextDeclOrder {0};

    // Map of variable nodes
    std::unordered_map<const VarDecl*, VarNode*> _varNodesMap;

    // Map of node filters
    std::unordered_map<const VarNode*, FilterNode*> _nodeFiltersMap;
};

}
