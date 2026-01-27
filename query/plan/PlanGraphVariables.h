#pragma once

#include <unordered_map>
#include <vector>

namespace db {

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
    using ProducerMap = std::unordered_map<const VarDecl*, PlanGraphNode*>;

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

    void setProducer(const VarDecl* varDecl, PlanGraphNode* producer);
    PlanGraphNode* getProducer(const VarDecl* varDecl) const;

private:
    PlanGraph* _tree {nullptr};

    // List of all var nodes. Used for predictable order iteration
    std::vector<VarNode*> _varNodes;

    // Map of VarDecl -> VarNode
    VarNodeMap _varNodesMap;

    // Map of VarNode -> Filter
    FilterNodeMap _nodeFiltersMap;

    // Map of VarDecl -> PlanGraphNode
    // Used to track the producers of a VarDecl, even if they're not produced by a VarNode
    ProducerMap _producers;
};

}
