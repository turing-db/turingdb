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

    generatePatternElementOrigin(element->getRootEntity());

    const auto& chain = element->getElementChain();
    for (const auto& [edge, node] : chain) {
        generatePatternElementExpand(edge, node);
    }
}

void PlanGraphGenerator::generatePatternElementOrigin(const EntityPattern* pattern) {
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
}

void PlanGraphGenerator::generatePatternElementExpand(const EntityPattern* edge,
                                                      const EntityPattern* node) {
}

/*
void PlanGraphGenerator::planMatchCommand(const MatchCommand* cmd) {    
    const auto& matchTargets = cmd->getMatchTargets()->targets();
    for (const auto& target : matchTargets) {
        planMatchTarget(target);
    }
}

void PlanGraphGenerator::planMatchTarget(const MatchTarget* target) {
    const PathPattern* pattern = target->getPattern();
    const auto& path = pattern->elements();

    if (path.empty()) {
        throw PlannerException("Empty match target pattern");
    }

    PlanGraphNode* currentNode = planPathMatchOrigin(path[0]);

    const auto expandSteps = path | rv::drop(1) | rv::chunk(2);
    for (auto step : expandSteps) {
        const EntityPattern* edge = step[0];
        const EntityPattern* target = step[1];
        currentNode = planPathExpand(currentNode, edge, target);
    }
}

PlanGraphNode* PlanGraphGenerator::planPathMatchOrigin(const EntityPattern* pattern) {
    const VarExpr* var = pattern->getVar();
    const TypeConstraint* typeConstr = pattern->getTypeConstraint();
    const ExprConstraint* exprConstr = pattern->getExprConstraint();

    // Scan nodes
    PlanGraphNode* current = nullptr;
    if (typeConstr) {
        const LabelSet* labelSet = getOrCreateLabelSet(typeConstr);
        current = _tree.create<ScanNodesByLabelNode>(labelSet); 
    } else {
        current = _tree.create<ScanNodesNode>();
    }

    // Expression constraints
    if (exprConstr) {
        const auto& exprs = exprConstr->getExpressions();
        for (const BinExpr* expr : exprs) {
            auto filter = _tree.create<FilterNodeExprNode>(expr);

            current->connectOut(filter);
            current = filter;
        }
    }

    // Variable declaration
    if (var) {
        auto* varNode = getOrCreateVarNode(var->getDecl());
        
        current->connectOut(varNode);
        current = varNode;
    }

    return current;
}

PlanGraphNode* PlanGraphGenerator::planPathExpand(PlanGraphNode* currentNode,
                                                  const EntityPattern* edge,
                                                  const EntityPattern* target) {
    // Get out edges
    auto getOutEdges = _tree.create<GetOutEdgesNode>();
    currentNode->connectOut(getOutEdges);
    currentNode = getOutEdges;

    // Edge constraints
    const TypeConstraint* edgeTypeConstr = edge->getTypeConstraint();
    const ExprConstraint* edgeExprConstr = edge->getExprConstraint();

    if (edgeTypeConstr) {
        // Search edge type IDs
        const auto& edgeTypeMap = _view.metadata().edgeTypes();
        const auto& edgeTypeNames = edgeTypeConstr->getTypeNames();

        const std::string& edgeTypeName = edgeTypeNames.front()->getName();
        const auto edgeTypeID = edgeTypeMap.get(edgeTypeName);
        if (!edgeTypeID) {
            throw PlannerException("Edge type " + edgeTypeName + " not found");
        }

        auto filterEdgeTypes = _tree.create<FilterEdgeTypeNode>(edgeTypeID.value());
        currentNode->connectOut(filterEdgeTypes);
        currentNode = filterEdgeTypes;
    }

    if (edgeExprConstr) {
        const auto& exprs = edgeExprConstr->getExpressions();
        for (const BinExpr* expr : exprs) {
            auto filterExpr = _tree.create<FilterEdgeExprNode>(expr);
            currentNode->connectOut(filterExpr);
            currentNode = filterExpr;
        }
    }

    const VarExpr* edgeVar = edge->getVar();
    if (edgeVar) {
        auto* varNode = getOrCreateVarNode(edgeVar->getDecl());
        currentNode->connectOut(varNode);
        currentNode = varNode;
    }

    // Target nodes
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();
    const ExprConstraint* targetExprConstr = target->getExprConstraint();

    auto getTargetNodes = _tree.create<GetEdgeTargetNode>();
    currentNode->connectOut(getTargetNodes);
    currentNode = getTargetNodes;

    // Target node constraints
    if (targetTypeConstr) {
        const LabelSet* labelSet = getOrCreateLabelSet(targetTypeConstr);

        auto filterLabel = _tree.create<FilterNodeLabelNode>(labelSet);

        currentNode->connectOut(filterLabel);
        currentNode = filterLabel;
    }

    if (targetExprConstr) {
        const auto& exprs = targetExprConstr->getExpressions();
        for (const BinExpr* expr : exprs) {
            auto filterExpr = _tree.create<FilterNodeExprNode>(expr);
            currentNode->connectOut(filterExpr);
            currentNode = filterExpr;
        }
    }

    const VarExpr* nodeVar = target->getVar();
    if (nodeVar) {
        auto* varNode = getOrCreateVarNode(nodeVar->getDecl());
        currentNode->connectOut(varNode);
        currentNode = varNode;
    }

    return currentNode;
}

// ===== CREATE =====

void PlanGraphGenerator::planCreateCommand(const CreateCommand* cmd) {
    const auto& createTargets = cmd->createTargets();
    for (const auto& target : createTargets) {
        planCreateTarget(target);
    }
}

void PlanGraphGenerator::planCreateTarget(const CreateTarget* target) {
    const PathPattern* pattern = target->getPattern();
    const auto& path = pattern->elements();
    
    PlanGraphNode* currentNode = planCreateOrigin(path[0]);

    const auto createSteps = path | rv::drop(1) | rv::chunk(2);
    for (const auto step : createSteps) {
        const EntityPattern* edge = step[0];
        const EntityPattern* target = step[1];
        currentNode = planCreateStep(currentNode, edge, target);
    }
}

PlanGraphNode* PlanGraphGenerator::planCreateOrigin(const EntityPattern* pattern) {
    const VarExpr* var = pattern->getVar();
    const TypeConstraint* typeConstr = pattern->getTypeConstraint();
    const ExprConstraint* exprConstr = pattern->getExprConstraint();

    PlanGraphNode* varNode = nullptr;
    if (var) {
        // Return the var node if it already exists, no need to create a new node
        const auto foundVarNode = getVarNode(var->getDecl());
        if (foundVarNode) {
            if (typeConstr || exprConstr) {
                throw PlannerException("Variable "+var->getName()+" is already bound");
            }

            return foundVarNode;
        } else {
            varNode = _tree.create<VarNode>(var->getDecl());
            addVarNode(varNode, var->getDecl());
        }
    }

    // If we are here, we need to create a new node
    // and maybe connect it to the var node if it exists
    CreateNodeNode* createNode = _tree.create<CreateNodeNode>();
    if (varNode) {
        createNode->connectOut(varNode);
    }

    if (typeConstr) {
        const LabelSet* labelSet = getOrCreateLabelSet(typeConstr);
        createNode->setLabelSet(labelSet);
    }

    createNode->setExprConstraint(exprConstr);

    return createNode;
}

PlanGraphNode* PlanGraphGenerator::planCreateStep(PlanGraphNode* currentNode,
                                              const EntityPattern* edge,
                                              const EntityPattern* target) {
    // Check that the edge variable is not already bound
    const VarExpr* edgeVar = edge->getVar();
    PlanGraphNode* edgeVarNode = nullptr;
    if (edgeVar) {
        const auto foundEdgeVarNode = getVarNode(edgeVar->getDecl());
        if (foundEdgeVarNode) {
            throw PlannerException("Variable for edge "+edgeVar->getName()+" is already bound");
        } else {
            edgeVarNode = _tree.create<VarNode>(edgeVar->getDecl());
            addVarNode(edgeVarNode, edgeVar->getDecl());
        }
    }

    // Search if the target variable already exists
    const VarExpr* targetVar = target->getVar();
    const TypeConstraint* targetTypeConstr = target->getTypeConstraint();
    const ExprConstraint* targetExprConstr = target->getExprConstraint();

    PlanGraphNode* foundTargetVarNode = nullptr;
    if (targetVar) {
        foundTargetVarNode = getVarNode(targetVar->getDecl());
        // If the target variable is already bound, it must not have any constraints
        if (foundTargetVarNode) {
            if (targetTypeConstr || targetExprConstr) {
                throw PlannerException("Variable for target "+targetVar->getName()+" is already bound");
            }
        }
    }

    PlanGraphNode* targetNode = nullptr;
    if (!foundTargetVarNode) {
        // If the target node is not already bound to an existing variable, we need to create a new node
        targetNode = _tree.create<CreateNodeNode>();

        if (targetVar) {
            // And if there is a variable requested, we need to create a new var node
            auto targetVarNode = _tree.create<VarNode>(targetVar->getDecl());
            addVarNode(targetVarNode, targetVar->getDecl());
            targetNode->connectOut(targetVarNode);
            targetNode = targetVarNode;
        }
    } else {
        targetNode = foundTargetVarNode;
    }

    // Create the edge
    PlanGraphNode* createEdge = _tree.create<CreateEdgeNode>();
    currentNode->connectOut(createEdge);
    targetNode->connectOut(createEdge);

    if (edgeVarNode) {
        createEdge->connectOut(edgeVarNode);
    }
    
    return targetNode;
}

// ===== CREATE GRAPH =====

void PlanGraphGenerator::planCreateGraphCommand(const CreateGraphCommand* cmd) {
    _tree.create<CreateGraphNode>(cmd->getName());
}

// ===== Labels and labelsets utilities =====
*/
