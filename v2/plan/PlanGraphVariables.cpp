#include "PlanGraphVariables.h"

#include "PlanGraph.h"
#include "PlannerException.h"
#include "decl/VarDecl.h"
#include "nodes/VarNode.h"
#include "nodes/FilterNode.h"
#include "BioAssert.h"

using namespace db::v2;

PlanGraphVariables::PlanGraphVariables(PlanGraph& tree)
    : _tree(&tree)
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

std::tuple<VarNode*, FilterNode*> PlanGraphVariables::getOrCreateVarNodeAndFilter(const VarDecl* varDecl) {
    VarNode* varNode = getVarNode(varDecl);
    FilterNode* filterNode = getNodeFilter(varNode);

    // Either varNode and FilterNode both exist, or neither exist
    bioassert((varNode && filterNode) || (!varNode && !filterNode));

    if (!varNode) {

        // Create varNode/filterNode pair
        varNode = _tree->create<VarNode>(varDecl);
        FilterNode* filterNode = nullptr;

        if (varDecl->getType() == EvaluatedType::NodePattern) {
            filterNode = _tree->create<FilterNodeNode>();
        } else if (varDecl->getType() == EvaluatedType::EdgePattern) {
            filterNode = _tree->create<FilterEdgeNode>();
        } else {
            throw PlannerException("Unsupported variable type");
        }

        _varNodesMap[varDecl] = varNode;
        _nodeFiltersMap[varNode] = filterNode;

        filterNode->connectOut(varNode);

        return {varNode, filterNode};
    }

    return {varNode, filterNode};
}
