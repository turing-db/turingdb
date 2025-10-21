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
    explicit PlanGraphVariables(PlanGraph* tree);
    ~PlanGraphVariables();

    using VarNodeMap = std::unordered_map<const VarDecl*, VarNode*>;
    using FilterNodeMap = std::unordered_map<const VarNode*, FilterNode*>;

    struct FilterNodeMapRange {
        FilterNodeMap::const_iterator _begin;
        FilterNodeMap::const_iterator _end;

        FilterNodeMap::const_iterator begin() const {
            return _begin;
        }

        FilterNodeMap::const_iterator end() const {
            return _end;
        }
    };

    PlanGraphVariables(const PlanGraphVariables&) = delete;
    PlanGraphVariables(PlanGraphVariables&&) = delete;
    PlanGraphVariables& operator=(const PlanGraphVariables&) = delete;
    PlanGraphVariables& operator=(PlanGraphVariables&&) = delete;

    VarNode* getVarNode(const VarDecl* varDecl) const;
    FilterNode* getNodeFilter(const VarNode* varNode) const;

    std::tuple<VarNode*, FilterNode*> getVarNodeAndFilter(const VarDecl* varDecl);
    std::tuple<VarNode*, FilterNode*> createVarNodeAndFilter(const VarDecl* varDecl);

    FilterNodeMapRange getNodeFiltersMap() {
        return {_nodeFiltersMap.begin(), _nodeFiltersMap.end()};
    }

private:
    PlanGraph* _tree {nullptr};

    // Map of variable nodes
    VarNodeMap _varNodesMap;

    // Map of node filters
    FilterNodeMap _nodeFiltersMap;
};

}
