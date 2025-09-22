#include "PlanGraphGenerator.h"

#include <spdlog/fmt/bundled/core.h>

#include "CypherAST.h"
#include "CypherError.h"
#include "WherePredicate.h"
#include "QualifiedName.h"
#include "WhereClause.h"
#include "decl/VarDecl.h"
#include "views/GraphView.h"

#include "nodes/FilterNode.h"
#include "nodes/GetEdgeTargetNode.h"
#include "nodes/GetEdgesNode.h"
#include "nodes/GetInEdgesNode.h"
#include "nodes/GetOutEdgesNode.h"
#include "nodes/ScanNodesNode.h"
#include "nodes/VarNode.h"
#include "nodes/ProjectResultsNode.h"

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
      _ast(&ast),
     _variables(_tree)
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

void PlanGraphGenerator::generate(const QueryCommand* query) {
    switch (query->getKind()) {
        case QueryCommand::Kind::SINGLE_PART_QUERY:
            generateSinglePartQuery(static_cast<const SinglePartQuery*>(query));
            break;

        default:
            throwError("Unsupported query command of type "
                           + std::to_string((unsigned)query->getKind()),
                       query);
            break;
    }
}

void PlanGraphGenerator::generateSinglePartQuery(const SinglePartQuery* query) {
    const StmtContainer* readStmts = query->getReadStmts();
    for (const Stmt* stmt : readStmts->stmts()) {
        generateStmt(stmt);
    }

    const StmtContainer* updateStmts = query->getUpdateStmts();
    if (updateStmts) {
        throwError("Update statements are not supported yet", query);
    }

    const ReturnStmt* returnStmt = query->getReturnStmt();
    if (!returnStmt) {
        throwError("Return statement is missing", query);
    }

    generateReturnStmt(returnStmt);
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
        generateWhereClause(where);
    }
}

void PlanGraphGenerator::generateReturnStmt(const ReturnStmt* stmt) {
    auto* projectResults = static_cast<PlanGraphNode*>(_tree.create<ProjectResultsNode>());

    for (const auto& end : _endPoints) {
        end->connectOut(projectResults);
    }
}

void PlanGraphGenerator::generateWhereClause(const WhereClause* where) {
    const Expr* expr = where->getExpr();

    unwrapWhereExpr(expr);
}

void PlanGraphGenerator::generatePatternElement(const PatternElement* element) {
    if (element->size() == 0) {
        throwError("Empty match pattern element", element);
    }

    const NodePattern* origin = dynamic_cast<const NodePattern*>(element->getRootEntity());
    if (!origin) {
        throwError("Pattern element origin must be a node pattern", element);
    }

    // Reset the decl order of variables (each branch starts at 0)
    _variables.resetDeclOrder();

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

    _endPoints.push_back(currentNode);
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementOrigin(const NodePattern* origin) {
    const NodePatternData* data = origin->getData();
    const LabelSet& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = origin->getDecl();

    // Scan nodes
    PlanGraphNode* currentNode = _tree.create<ScanNodesNode>();

    const auto [var, filter] = _variables.getOrCreateVarNodeAndFilter(decl);
    currentNode->connectOut(filter);
    currentNode = var;

    auto* typedFilter = static_cast<FilterNodeNode*>(filter);

    typedFilter->addLabelConstraints(labelset);

    for (const auto& [propType, expr] : exprConstraints) {
        typedFilter->addPropertyConstraint(propType._id, expr, BinaryOperator::Equal);
    }

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementEdge(PlanGraphNode* currentNode,
                                                              const EdgePattern* edge) {
    // Expand edge based on direction

    switch (edge->getDirection()) {
        case EdgePattern::Direction::Undirected: {
            currentNode = _tree.newOut<GetEdgesNode>(currentNode);
            break;
        }
        case EdgePattern::Direction::Backward: {
            currentNode = _tree.newOut<GetInEdgesNode>(currentNode);
            break;
        } break;
        case EdgePattern::Direction::Forward: {
            currentNode = _tree.newOut<GetOutEdgesNode>(currentNode);
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

    auto [var, filter] = _variables.getOrCreateVarNodeAndFilter(decl);
    currentNode->connectOut(filter);
    currentNode = var;

    auto* typedFilter = static_cast<FilterEdgeNode*>(filter);

    for (const auto& [propType, expr] : exprConstraints) {
        typedFilter->addPropertyConstraint(propType._id, expr, BinaryOperator::Equal);
    }

    for (const EdgeTypeID edgeTypeID : edgeTypes) {
        typedFilter->addEdgeTypeConstraint(edgeTypeID);
    }

    return currentNode;
}

PlanGraphNode* PlanGraphGenerator::generatePatternElementTarget(PlanGraphNode* currentNode,
                                                                const NodePattern* target) {
    // Target nodes
    const NodePatternData* data = target->getData();
    const auto& labelset = data->labelConstraints();
    const auto& exprConstraints = data->exprConstraints();
    const VarDecl* decl = target->getDecl();

    currentNode = _tree.newOut<GetEdgeTargetNode>(currentNode);

    auto [var, filter] = _variables.getOrCreateVarNodeAndFilter(decl);
    currentNode->connectOut(filter);
    currentNode = var;

    auto* typedFilter = static_cast<FilterNodeNode*>(filter);

    typedFilter->addLabelConstraints(labelset);

    for (const auto& [propType, expr] : exprConstraints) {
        typedFilter->addPropertyConstraint(propType._id, expr, BinaryOperator::Equal);
    }

    return currentNode;
}

void PlanGraphGenerator::unwrapWhereExpr(const Expr* expr) {
    if (expr->getKind() == Expr::Kind::NODE_LABEL) {
        // Node label expression can be pushed down to the var node
        //
        // WARNING
        // NODE_LABEL is missleading
        // It can also refer to an edge type such as: e:KNOWS

        const NodeLabelExpr* entityTypeExpr = static_cast<const NodeLabelExpr*>(expr);
        const VarDecl* decl = entityTypeExpr->getDecl();
        const VarNode* varNode = _variables.getVarNode(decl);
        FilterNode* filter = _variables.getNodeFilter(varNode);

        if (decl->getType() == EvaluatedType::NodePattern) {
            FilterNodeNode* nodeFilter = static_cast<FilterNodeNode*>(filter);
            nodeFilter->addLabelConstraints(entityTypeExpr->labelSet());

        } else if (decl->getType() == EvaluatedType::EdgePattern) {
            FilterEdgeNode* edgeFilter = static_cast<FilterEdgeNode*>(filter);

            if (entityTypeExpr->labels().size() != 1) {
                throwError("Only one edge type constraint is supported for now", expr);
            }

            // TODO: Fix, this is ugly, we make the NodeLabel expression Node/Edge agnostic
            // Right now we have to interprete "Label ids" as edge types
            std::vector<LabelID> edgeTypes;
            entityTypeExpr->labelSet().decompose(edgeTypes);
            for (const auto& edgeType : edgeTypes) {
                edgeFilter->addEdgeTypeConstraint(edgeType);
            }
        }

        return;
    }

    if (expr->getKind() == Expr::Kind::BINARY) {
        const BinaryExpr* binaryExpr = static_cast<const BinaryExpr*>(expr);

        if (binaryExpr->getOperator() == BinaryOperator::And) {
            // If AND operator, we can unwrap to push down predicates to var nodes
            unwrapWhereExpr(binaryExpr->getLHS());
            unwrapWhereExpr(binaryExpr->getRHS());
            return;
        }

        // --> Fallthrough
        //
        // If any other binary operator, we treat it as a whole predicate on
        // which we need to generate dependencies
    }

    // Unwraped the first list of AND expressions,
    // Treating other cases as a whole Where predicate
    WherePredicate predicate {expr};
    predicate.generate();

    uint32_t _lastDeclOrder = 0;
    const VarNode* _lastDependency = nullptr;

    for (const auto& dep : predicate.getDependencies()) {
        const VarDecl* decl = dep._decl;
        const VarNode* var = _variables.getVarNode(decl);

        if (_lastDeclOrder <= var->getDeclOdrer()) {
            _lastDeclOrder = var->getDeclOdrer();
            _lastDependency = var;
        }
    }

    if (!_lastDependency) {
        throwError("Where clauses without dependencies are not supported yet", expr);
    }

    FilterNode* filterNode = _variables.getNodeFilter(_lastDependency);
    filterNode->addWherePredicate(std::move(predicate));
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

