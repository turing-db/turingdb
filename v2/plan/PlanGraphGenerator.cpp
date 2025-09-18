#include "PlanGraphGenerator.h"

#include <spdlog/fmt/bundled/core.h>

#include "CypherAST.h"
#include "CypherError.h"
#include "FilterNode.h"
#include "GetNodeLabelSetNode.h"
#include "GetPropertyNode.h"
#include "PropertyMapExprNode.h"
#include "QualifiedName.h"
#include "views/GraphView.h"

#include "expr/PropertyExpr.h"
#include "expr/BinaryExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/StringExpr.h"
#include "expr/NodeLabelExpr.h"
#include "expr/PathExpr.h"
#include "expr/SymbolExpr.h"
#include "expr/LiteralExpr.h"

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

PlanGraphGenerator::PlanGraphGenerator(const CypherAST& ast,
                                       const GraphView& view,
                                       const QueryCallback& callback)
    : _view(view),
    _ast(&ast)
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
            throwError(fmt::format("Unsupported node label: {}", symbol->getName()), symbol);
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

void PlanGraphGenerator::addVarNode(PlanGraphNode* node, const VarDecl* varDecl) {
    _varNodesMap[varDecl] = node;
}

PlanGraphNode* PlanGraphGenerator::getVarNode(const VarDecl* varDecl) const {
    const auto it = _varNodesMap.find(varDecl);
    if (it == _varNodesMap.end()) {
        return nullptr;
    }

    return it->second;
}

PlanGraphNode* PlanGraphGenerator::getOrCreateVarNode(const VarDecl* varDecl) {
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
            throwError("Unsupported query command of type "
                       + std::to_string((unsigned)query->getKind()), query);
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
            throwError(fmt::format("Unsupported statement type: {}", (unsigned)stmt->getKind()), stmt);
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

    const WhereClause* where = pattern->getWhere();
    if (where) {
        throwError("WHERE clause not supported yet", where);
    }
}

void PlanGraphGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

    PlanGraphNode* currentNode = generatePatternElementOrigin(origin);

    const auto& chain = element->getElementChain();
    for (const auto& [edge, node] : chain) {
        const EdgePattern* e = dynamic_cast<const EdgePattern*>(edge);
        if (!edge) {
            throwError("Pattern element edge must be an edge pattern", element);
        }

        currentNode = generatePatternElementEdge(currentNode, e);

        const NodePattern* n = dynamic_cast<const NodePattern*>(node);
        if (!node) {
            throwError("Pattern element node must be a node pattern", element);
        }

        currentNode = generatePatternElementTarget(currentNode, n);
    }
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    const NodePatternData* data = origin->getData();
    const LabelSet& labelSet = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = origin->getDecl();

    // Scan nodes
    PlanGraphNode* currentNode = nullptr;
    if (labelSet.empty()) {
        currentNode = _tree.create<ScanNodesNode>();
    } else {
        currentNode = _tree.create<ScanNodesByLabelNode>(&labelSet);
    }

    currentNode = generateExprConstraints(currentNode, decl, exprConstraints);
    currentNode = generateVarNode(currentNode, decl);

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementEdge(PlanGraphNode* currentNode,
                                                              const EdgePattern* edge) {
    // Expand edge based on direction

    switch (edge->getDirection()) {
        case EdgePattern::Direction::Undirected:
        {
            auto* expandEdges = _tree.create<GetEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
        case EdgePattern::Direction::Backward:
        {
            auto* expandEdges = _tree.create<GetInEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
            break;
        case EdgePattern::Direction::Forward:
        {
            auto* expandEdges = _tree.create<GetOutEdgesNode>();
            currentNode->connectOut(expandEdges);
            currentNode = expandEdges;
            break;
        }
    }

    // Edge constraints
    const EdgePatternData* data = edge->getData();
    const auto& edgeTypes = data->edgeTypeConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = edge->getDecl();

    if (edgeTypes.size() > 1) {
        throwError("Only one edge type constraint is supported for now", edge);
    }

    for (const EdgeTypeID edgeTypeID : edgeTypes) {
        auto* filterEdgeTypes = _tree.create<FilterEdgeTypeNode>(edgeTypeID);
        currentNode->connectOut(filterEdgeTypes);
        currentNode = filterEdgeTypes;
    }

    currentNode = generateExprConstraints(currentNode, decl, exprConstraints);
    currentNode = generateVarNode(currentNode, decl);

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementTarget(PlanGraphNode* currentNode,
                                                                const NodePattern* target) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const auto& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();

    auto* getTargetNode = _tree.create<GetEdgeTargetNode>();
    currentNode->connectOut(getTargetNode);
    currentNode = getTargetNode;

    // Target node labels
    if (!labelset.empty()) {
        FilterNodeLabelNode* filterLabel = _tree.create<FilterNodeLabelNode>(&labelset);
        currentNode->connectOut(filterLabel);
        currentNode = filterLabel;
    }

    currentNode = generateExprConstraints(currentNode, decl, exprConstraints);
    currentNode = generateVarNode(currentNode, decl);

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generateExprConstraints(PlanGraphNode* currentNode,
                                                           const VarDecl* varDecl,
                                                           const ExprConstraints& exprConstraints) {
    if (exprConstraints.empty()) {
        return currentNode;
    }

    PlanGraphNode* prevNode = currentNode;

    // Node that generates the mask based on all property constraints
    PropertyMapExprNode* propMapExpr = _tree.create<PropertyMapExprNode>();
    currentNode->connectOut(propMapExpr);
    currentNode = propMapExpr;

    // All dependencies are branches feeding into propMapExpr, e.g. for property expressions
    //
    //       ->  GetProperty ----
    //       |                  |
    // varNode -> * -> prev -> propMapExpr -> filter

    for (const auto& [propType, expr] : exprConstraints) {
        auto* getPropertyNode = _tree.create<GetPropertyNode>();
        prevNode->connectOut(getPropertyNode);
        getPropertyNode->connectOut(currentNode);

        propMapExpr->addExpr(propType._id, expr);
        generateExprDependencies(currentNode, expr);
    }

    // Node that applies the mask to whatever needs it
    FilterNode* filter = _tree.create<FilterNode>();
    propMapExpr->connectOut(filter);
    currentNode = filter;

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generateVarNode(PlanGraphNode* currentNode, const VarDecl* varDecl) {
    if (varDecl) {
        auto* varNode = getOrCreateVarNode(varDecl);
        currentNode->connectOut(varNode);
        currentNode = varNode;
    }

    return currentNode;
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY: {
            generateExprDependencies(currentNode, static_cast<const BinaryExpr*>(expr));
        } break;
        case Expr::Kind::UNARY: {
            generateExprDependencies(currentNode, static_cast<const UnaryExpr*>(expr));
        } break;
        case Expr::Kind::STRING: {
            generateExprDependencies(currentNode, static_cast<const StringExpr*>(expr));
        } break;
        case Expr::Kind::NODE_LABEL: {
            generateExprDependencies(currentNode, static_cast<const NodeLabelExpr*>(expr));
        } break;
        case Expr::Kind::PROPERTY: {
            generateExprDependencies(currentNode, static_cast<const PropertyExpr*>(expr));
        } break;
        case Expr::Kind::PATH: {
            generateExprDependencies(currentNode, static_cast<const PathExpr*>(expr));
        } break;
        case Expr::Kind::SYMBOL: {
            generateExprDependencies(currentNode, static_cast<const SymbolExpr*>(expr));
        } break;
        case Expr::Kind::LITERAL: {
            generateExprDependencies(currentNode, static_cast<const LiteralExpr*>(expr));
        } break;
    }
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const BinaryExpr* expr) {
    generateExprDependencies(currentNode, expr->getLHS());
    generateExprDependencies(currentNode, expr->getRHS());
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const UnaryExpr* expr) {
    generateExprDependencies(currentNode, expr->getSubExpr());
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const StringExpr* expr) {
    generateExprDependencies(currentNode, expr->getLHS());
    generateExprDependencies(currentNode, expr->getRHS());
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const NodeLabelExpr* expr) {
    const VarDecl* varDecl = expr->getDecl();
    if (!varDecl) {
        throwError("Node label expression is missing its variable declaration", expr);
    }

    PlanGraphNode* varNode = getVarNode(varDecl);
    if (!varNode) {
        throwError("Node label expression refers to an undefined variable", expr);
    }

    auto* getNodeLabelNode = _tree.create<GetNodeLabelSetNode>();
    varNode->connectOut(getNodeLabelNode);
    getNodeLabelNode->connectOut(currentNode);
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const PropertyExpr* expr) {
    const VarDecl* varDecl = expr->getDecl();
    if (!varDecl) {
        throwError("Property expression is missing its variable declaration", expr);
    }

    PlanGraphNode* varNode = getVarNode(varDecl);
    if (!varNode) {
        throwError("Property expression refers to an undefined variable", expr);
    }

    auto* getPropertyNode = _tree.create<GetPropertyNode>();
    varNode->connectOut(getPropertyNode);
    getPropertyNode->connectOut(currentNode);
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const PathExpr* expr) {
    throwError("Path expressions are not supported yet", expr);
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const SymbolExpr* expr) {
    throwError("Symbol expressions are not supported yet", expr);
}

void PlanGraphGenerator::generateExprDependencies(PlanGraphNode* currentNode, const LiteralExpr* expr) {
    // No dependencies
}

void PlanGraphGenerator::throwError(std::string_view msg, const void* obj) const {
    const SourceLocation* location = _ast->getLocation(obj);
    std::string errorMsg;

    CypherError err {_ast->getQueryString()};
    err.setTitle("Query plan error");
    err.setErrorMsg(msg);

    if (location) {
        err.setLocation(*location);
    }

    err.generate(errorMsg);

    throw PlannerException(std::move(errorMsg));
}
