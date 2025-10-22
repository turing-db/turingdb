#pragma once

#include <unordered_map>
#include <vector>

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

    PlanGraphVariables(const PlanGraphVariables&) = delete;
    PlanGraphVariables(PlanGraphVariables&&) = delete;
    PlanGraphVariables& operator=(const PlanGraphVariables&) = delete;
    PlanGraphVariables& operator=(PlanGraphVariables&&) = delete;

    VarNode* getVarNode(const VarDecl* varDecl) const;
    FilterNode* getNodeFilter(const VarNode* varNode) const;

    std::tuple<VarNode*, FilterNode*> getVarNodeAndFilter(const VarDecl* varDecl);
    std::tuple<VarNode*, FilterNode*> createVarNodeAndFilter(const VarDecl* varDecl);

    const std::vector<VarNode*>& getVarNodes() const {
        return _varNodes;
    };

private:
    PlanGraph* _tree {nullptr};

    // List of all var nodes. Used for predictable order iteration
    std::vector<VarNode*> _varNodes;

    // Map of VerDecl -> VarNode
    VarNodeMap _varNodesMap;

    // Map of VarNode -> Filter
    FilterNodeMap _nodeFiltersMap;
};

}
