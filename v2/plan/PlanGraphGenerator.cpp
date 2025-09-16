#include "PlanGraphGenerator.h"

#include <range/v3/view/drop.hpp>
#include <range/v3/view/chunk.hpp>
#include <spdlog/fmt/bundled/core.h>

#include "views/GraphView.h"

#include "PlanGraph.h"
#include "QueryCommand.h"
#include "SinglePartQuery.h"
#include "stmt/StmtContainer.h"
#include "stmt/Stmt.h"
#include "stmt/MatchStmt.h"
#include "Pattern.h"
#include "PatternElement.h"
#include "NodePattern.h"
#include "decl/PatternData.h"

#include "Symbol.h"

#include "PlannerException.h"

using namespace db::v2;
using namespace db;

PlanGraphGenerator::PlanGraphGenerator(const GraphView& view,
                                       QueryCallback callback)
    : _view(view)
{
}

PlanGraphGenerator::~PlanGraphGenerator() {
}

const LabelSet* PlanGraphGenerator::getOrCreateLabelSet(const Symbols& symbols) {
    LabelNames labelNames;
    for (const Symbol* symbol : symbols) {
        labelNames.push_back(symbol->getName());
    }

    const auto labelSetCacheIt = _labelSetCache.find(labelNames);
    if (labelSetCacheIt != _labelSetCache.end()) {
        return labelSetCacheIt->second;
    }

    const LabelSet* labelSet = buildLabelSet(symbols);
    _labelSetCache[labelNames] = labelSet;

    return labelSet;
}

const LabelSet* PlanGraphGenerator::buildLabelSet(const Symbols& symbols) {
    auto labelSet = std::make_unique<LabelSet>();
    for (const Symbol* symbol : symbols) {
        const LabelID labelID = getLabel(symbol);

        if (!labelID.isValid()) {
            throw PlannerException(fmt::format("Unsupported node label: {}", symbol->getName()));
        }

        labelSet->set(labelID);
    }

    LabelSet* ptr = labelSet.get();
    _labelSets.push_back(std::move(labelSet));

    return ptr;
}

LabelID PlanGraphGenerator::getLabel(const Symbol* symbol) {
    auto res = _view.metadata().labels().get(symbol->getName());
    if (!res) {
        return LabelID {};
    }

    return res.value();
}

// ===== Var nodes utilities =====

void PlanGraphGenerator::addVarNode(PlanGraphNode* node, VarDecl* varDecl) {
    _varNodesMap[varDecl] = node;
}

PlanGraphNode* PlanGraphGenerator::getVarNode(VarDecl* varDecl) const {
    const auto it = _varNodesMap.find(varDecl);
    if (it == _varNodesMap.end()) {
        return nullptr;
    }

    return it->second;
}

PlanGraphNode* PlanGraphGenerator::getOrCreateVarNode(VarDecl* varDecl) {
    PlanGraphNode* varNode = getVarNode(varDecl);
    if (!varNode) {
        varNode = _tree.create<VarNode>(varDecl);
        addVarNode(varNode, varDecl);
    }

    return varNode;
}

void PlanGraphGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            generateSinglePartQuery(static_cast<const SinglePartQuery*>(query));
            break;

        default:
            throw PlannerException("Unsupported query command of type "
                                   + std::to_string((unsigned)query->getKind()));
            break;
    }
}

void PlanGraphGenerator::generateSinglePartQuery(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    for (const Stmt* stmt : readStmts->stmts()) {
        generateStmt(stmt);
    }

    // TODO: Return statement
}

void PlanGraphGenerator::generateStmt(const Stmt* stmt) {
    switch (stmt->getKind()) {
        case Stmt::Kind::MATCH:
            generateMatchStmt(static_cast<const MatchStmt*>(stmt));
            break;

        default:
            throw PlannerException(fmt::format("Unsupported statement type: {}", (unsigned)stmt->getKind()));
            break;
    }
}

void PlanGraphGenerator::generateMatchStmt(const MatchStmt* stmt) {
    const Pattern* pattern = stmt->getPattern();

    // Each PatternElement is a target of the match
    // and contains a chain of EntityPatterns
    for (const PatternElement* element : pattern->elements()) {
        generatePatternElement(element);
    }
}

void PlanGraphGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throw PlannerException("Empty match pattern element");
    }

    PlanGraphNode* currentNode = generatePatternElementOrigin(element->getRootEntity());

    const auto& chain = element->getElementChain();
    for (const auto& [edge, node] : chain) {
        currentNode = generatePatternElementExpand(currentNode, edge, node);
    }
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementOrigin(const EntityPattern* pattern) {
    const NodePattern* nodePattern = dynamic_cast<const NodePattern*>(pattern);
    if (!nodePattern) {
        throw PlannerException("Pattern element origin must be a node pattern");
    }

    const NodePatternData* data = nodePattern->getData();
    const LabelSet& labelSet = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();

    // Scan nodes
    PlanGraphNode* currentNode = nullptr;
    if (labelSet.empty()) {
        currentNode = _tree.create<ScanNodesNode>();
    } else {
        currentNode = _tree.create<ScanNodesByLabelNode>(&labelSet);
    }

    // Expression constraints
    for (const auto& [propType, expr] : exprConstraints) {
        FilterNodeExprNode* filter = _tree.create<FilterNodeExprNode>(expr);
        currentNode->connectOut(filter);
        currentNode = filter;
    }

    // Variable declaration
    VarDecl* varDecl = nodePattern->getDecl();
    if (varDecl) {
        auto* varNode = getOrCreateVarNode(varDecl);
        currentNode->connectOut(varNode);
        currentNode = varNode;
    }

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementExpand(PlanGraphNode* currentNode,
                                                                const EntityPattern* edge,
                                                                const EntityPattern* target) {
    const EdgePattern* edgePattern = dynamic_cast<const EdgePattern*>(edge);
    if (!edgePattern) {
        throw PlannerException("Edge pattern element must be an edge pattern");
    }

    const NodePattern* targetPattern = dynamic_cast<const NodePattern*>(target);
    if (!targetPattern) {
        throw PlannerException("Target pattern element must be a node pattern");
    }
                                                        
    // Expand edge based on direction
    switch (edgePattern->getDirection()) {
        case EdgePattern::Direction::Undirected:
        {
            auto expandEdges = _tree.create<GetEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
        case EdgePattern::Direction::Backward:
        {
            auto expandEdges = _tree.create<GetInEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
            break;
        case EdgePattern::Direction::Forward:
        {
            auto expandEdges = _tree.create<GetOutEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
    }

    // Edge constraints
    const EdgePatternData* edgeData = edgePattern->getData();
    const auto& edgeTypes = edgeData->edgeTypeConstraints();

    for (const EdgeTypeID edgeTypeID : edgeTypes) {
        auto filterEdgeTypes = _tree.create<FilterEdgeTypeNode>(edgeTypeID);
        currentNode->connectOut(filterEdgeTypes);
        currentNode = filterEdgeTypes;
    }

    // Expression constraints
    const auto& exprConstraints = edgeData->exprConstraints();
    for (const auto& [propType, expr] : exprConstraints) {
        FilterEdgeExprNode* filter = _tree.create<FilterEdgeExprNode>(expr);
        currentNode->connectOut(filter);
        currentNode = filter;
    }

    // Edge variable declaration
    VarDecl* edgeVarDecl = edgePattern->getDecl();
    if (edgeVarDecl) {
        PlanGraphNode* varNode = getOrCreateVarNode(edgeVarDecl);
        currentNode->connectOut(varNode);
        currentNode = varNode;
    }

    // Target nodes
    const NodePatternData* targetData = targetPattern->getData();
    const auto& targetLabelSet = targetData->labelConstraints();
    const auto& targetExprConstraints = targetData->exprConstraints();

    auto getTargetNodes = _tree.create<GetEdgeTargetNode>();
    currentNode->connectOut(getTargetNodes);
    currentNode = getTargetNodes;

    // Target node labels
    if (!targetLabelSet.empty()) {
        FilterNodeLabelNode* filterLabel = _tree.create<FilterNodeLabelNode>(&targetLabelSet);
        currentNode->connectOut(filterLabel);
        currentNode = filterLabel;
    }

    // Target node expression constraints
    for (const auto& [propType, expr] : targetExprConstraints) {
        FilterNodeExprNode* filter = _tree.create<FilterNodeExprNode>(expr);
        currentNode->connectOut(filter);
        currentNode = filter;
    }

    // Target node variable declaration
    VarDecl* targetVarDecl = targetPattern->getDecl();
    if (targetVarDecl) {
        PlanGraphNode* varNode = getOrCreateVarNode(targetVarDecl);
        currentNode->connectOut(varNode);
        currentNode = varNode;
    }

    return currentNode;
}
