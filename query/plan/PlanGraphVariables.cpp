#include "PlanGraphVariables.h"

#include "PlanGraph.h"
#include "PlannerException.h"
#include "decl/VarDecl.h"
#include "nodes/VarNode.h"
#include "nodes/FilterNode.h"

using namespace db;

PlanGraphVariables::PlanGraphVariables(PlanGraph* tree)
    : _tree(tree)
{
}

PlanGraphVariables::~PlanGraphVariables() = default;

VarNode* PlanGraphVariables::getVarNode(const VarDecl* varDecl) const {
    const auto it = _varNodesMap.find(varDecl);
    if (it == _varNodesMap.end()) {
        return nullptr;
    }

    return it->second;
}

FilterNode* PlanGraphVariables::getNodeFilter(const VarNode* varNode) const {
    const auto it = _nodeFiltersMap.find(varNode);
    if (it == _nodeFiltersMap.end()) {
        return nullptr;
    }

    return it->second;
}

std::tuple<VarNode*, FilterNode*> PlanGraphVariables::getVarNodeAndFilter(const VarDecl* varDecl) {
    const auto it = _varNodesMap.find(varDecl);
    if (it == _varNodesMap.end()) {
        return { nullptr, nullptr };
    }

    return {it->second, getNodeFilter(it->second)};
}

std::tuple<VarNode*, FilterNode*> PlanGraphVariables::createVarNodeAndFilter(const VarDecl* varDecl) {
    // Create varNode/filterNode pair
    VarNode* varNode = _tree->create<VarNode>(varDecl);
    FilterNode* filterNode = nullptr;

    if (varDecl->getType() == EvaluatedType::NodePattern) {
        filterNode = _tree->create<NodeFilterNode>();
    } else if (varDecl->getType() == EvaluatedType::EdgePattern) {
        filterNode = _tree->create<EdgeFilterNode>();
    } else {
        throw PlannerException("Unsupported variable type");
    }

    _varNodes.push_back(varNode);
    _varNodesMap[varDecl] = varNode;
    _nodeFiltersMap[varNode] = filterNode;

    filterNode->connectOut(varNode);
    filterNode->setVarNode(varNode);

    return {varNode, filterNode};
}

void PlanGraphVariables::setProducer(const VarDecl* varDecl, PlanGraphNode* producer) {
    _producers[varDecl] = producer;
}

PlanGraphNode* PlanGraphVariables::getProducer(const VarDecl* varDecl) const {
    const auto it = _producers.find(varDecl);
    if (it == _producers.end()) {
        return nullptr;
    }

    return it->second;
}
