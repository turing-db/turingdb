#include "ReturnStmtGenerator.h"

#include <queue>
#include <variant>
#include <vector>

#include <range/v3/view/drop.hpp>
#include <range/v3/view/reverse.hpp>

#include "CypherAST.h"
#include "ExprDependencies.h"
#include "FunctionInvocation.h"
#include "Literal.h"
#include "PlanGraph.h"
#include "Projection.h"

#include "QualifiedName.h"
#include "Symbol.h"
#include "decl/EvaluatedType.h"
#include "decl/VarDecl.h"
#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/LiteralExpr.h"
#include "expr/UnaryExpr.h"
#include "nodes/PlanGraphNode.h"
#include "stmt/Limit.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/ReturnStmt.h"
#include "stmt/Skip.h"

#include "nodes/AggregateEvalNode.h"
#include "nodes/ExprEvalNode.h"
#include "nodes/FuncEvalNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/LimitNode.h"
#include "nodes/OrderByNode.h"
#include "nodes/ProduceResultsNode.h"
#include "nodes/SkipNode.h"

#include "expr/EntityTypeExpr.h"
#include "expr/ExprChain.h"
#include "expr/FunctionInvocationExpr.h"
#include "expr/PropertyExpr.h"
#include "expr/SymbolExpr.h"

#include "DiagnosticsManager.h"

#include "BioAssert.h"
#include "PlannerException.h"

using namespace db;
namespace rg = ranges;
namespace rv = rg::views;

ReturnStmtGenerator::ReturnStmtGenerator(const CypherAST* ast,
                                         PlanGraphVariables* vars,
                                         const ReturnStmt* rtnStmt,
                                         PlanGraph* tree,
                                         PlanGraphNode* prevNode,
                                         GetPropertyCache& propCache)
    : _ast(ast),
    _vars(vars),
    _stmt(rtnStmt),
    _tree(tree),
    _prevNode(prevNode),
    _propCache(propCache)
{
}

void ReturnStmtGenerator::prepare() {
    _proj = _stmt->getProjection();
    bioassert(_proj, "Failed to get projection for RETURN statement.");
}

/// Helper to get all expressions in return items, and those in order by keys
static void getReturnExpressions(const Projection* proj, std::vector<Expr*>& exprs) {
    exprs.clear();
    for (const Projection::ReturnItem& item : proj->items()) {
         Expr* const* exprPtr = std::get_if<Expr*>(&item);
        if (!exprPtr) {
            continue;
        }

        Expr* returnExpression = *exprPtr;
        exprs.push_back(returnExpression);
    }

    if (!proj->hasOrderBy()) {
        return;
    }

    const OrderByNode::ItemVector& orderByItems = proj->getOrderBy()->getItems();
    for (const OrderByItem* item : orderByItems) {
        Expr* orderByExpression = item->getExpr();
        exprs.push_back(orderByExpression);
    }
}

PlanGraphNode* ReturnStmtGenerator::generateReturnStmt() {
    prepare();

    if (_proj->isDistinct()) {
        throwError("DISTINCT not yet supported.", _stmt);
    }

    _exprEvalNode = _tree->create<ExprEvalNode>();

    std::vector<Expr*> returnExpressions;
    getReturnExpressions(_proj, returnExpressions);

    // BFS from root; blocked by aggregates
    bioassert(_frontier.empty(), "Expression frontier was non-empty.");
    bioassert(_blockers.empty(), "Expression-blocking queue was non-empty.");
    for (Expr* root : returnExpressions) {
        _frontier.push(root);
    }

    while (!_frontier.empty() || !_blockers.empty()) {
        // Process all non-blocking expressions on the frontier
        while (!_frontier.empty()) {
            Expr* front = _frontier.front();
            _frontier.pop();

            // Evaluation blockers need be evaluated on their own level
            if (isEvaluationBlocker(front)) {
                _blockers.push(front);
                continue;
            }

            if (ExprEvalNode::needsEvaluation(front)) {
                if (!_exprEvalNode) {
                    _exprEvalNode = _tree->create<ExprEvalNode>();
                }
                _exprEvalNode->addExpr(front);
            }

            // Adds all children of @ref front to @ref _frontier, to continue BFS
            expandExpr(front);
        }

        // Process all blocking expressions on the frontier
        while (!_blockers.empty()) {
            const Expr* blocker = _blockers.front();
            _blockers.pop();

            handleEvaluationBlocker(blocker);
        }

        // We may have populated both @ref _aggrEvalNode and @ref _exprEvalNode.
        // If we have no aggregates (blockers), do not add an ExprEvalNode becuase we
        // may have more expressions to evaluate in following return items. If we have
        // aggregates, add the ExprEval and then the blocking AggregateEval.
        const bool haveAggrs = _aggrEvalNode && !_aggrEvalNode->getFuncs().empty();
        const bool needEvaluateExpr = _exprEvalNode && !_exprEvalNode->getExprs().empty();
        if (haveAggrs) {
            if (needEvaluateExpr) {
                _evalSteps.emplace_back(_exprEvalNode);
                _exprEvalNode = nullptr; // Reset to denote we need a new node
            }
            _evalSteps.emplace_back(_aggrEvalNode);
            _aggrEvalNode = nullptr; // Reset to denote we need a new node
        }
    }

    // Since we only add ExprEvalNode if we have aggregates, we may have a remaning
    // ExprEvalNode which was never added because there was no AggregateEval. Check, and
    // add if needed.
    const bool needEvaluate = _exprEvalNode && !_exprEvalNode->getExprs().empty();
    if (needEvaluate) {
        _evalSteps.emplace_back(_exprEvalNode);
    }

    // We explored each return expression from root to leaf, meaning the order of
    // @ref _evalSteps is the reverse order that the nodes need be evaluated in.
    if (!_evalSteps.empty()) {
        const auto addStepToPlan = [this](auto&& evalNode) {
            if (_prevNode) {
                _prevNode->connectOut(evalNode);
            }
            _prevNode = evalNode;
        };

        for (const EvaluationStep& evalStep : rv::reverse(_evalSteps)) {
            std::visit(addStepToPlan, evalStep);
        }
    }

    // ORDER BY, SKIP, LIMIT require a previous input, `LIMIT 10` is not a valid
    // query, but `MATCH (n) LIMIT 10` is (because it has SCAN NODES as a previous
    // input), and so is `RETURN 5 LIMIT 10` (it has EXPR EVAL as a previous input).
    // Therefore, we can only add thse projection properties if @ref prevNode is
    // valid.
    if (_prevNode && _proj->hasOrderBy()) {
        // Any expression dependencies, e.g.
        // `MATCH (n) RETURN n.age ORDER BY n.age + 10`
        // where `n.age + 10` need be evaluated first, are handled at the entrypoint
        // of this function, meaning at this point, if there is an ORDER BY, all
        // dependent columns will be registered.
        auto* orderBy = _tree->newOut<OrderByNode>(_prevNode);
        orderBy->setItems(_proj->getOrderBy()->getItems());
        _prevNode = orderBy;
    }

    if (_prevNode && _proj->hasSkip()) {
        auto* skip = _tree->newOut<SkipNode>(_prevNode);
        skip->setExpr(_proj->getSkip()->getExpr());
        _prevNode = skip;
    }

    if (_prevNode && _proj->hasLimit()) {
        auto* limit = _tree->newOut<LimitNode>(_prevNode);
        limit->setExpr(_proj->getLimit()->getExpr());
        _prevNode = limit;
    }

    auto* results = _tree->newOut<ProduceResultsNode>(_prevNode);

    results->setProjection(_proj);

    return results;
}

bool ReturnStmtGenerator::isEvaluationBlocker(const Expr* expr) {
    const auto* function = dynamic_cast<const FunctionInvocationExpr*>(expr);
    const bool isAggregate = function && function->isAggregate();

    const bool isBlocker = isAggregate /* || other blockers */;

    return isBlocker;
}

void ReturnStmtGenerator::expandExpr(Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY: {
            const auto* bin = static_cast<const BinaryExpr*>(expr);
            Expr* lhs = bin->getLHS();
            Expr* rhs = bin->getRHS();
            _frontier.push(lhs);
            _frontier.push(rhs);
        }
        break;

        case Expr::Kind::UNARY: {
            const auto* unary = static_cast<const UnaryExpr*>(expr);
            Expr* subExpr = unary->getSubExpr();
            _frontier.push(subExpr);
        }
        break;

        case Expr::Kind::LITERAL: {
            // Literals are added for eval in guarded call to @ref needsEvaluation
            // But lists need expanding for each of their elements
            const auto* lit = static_cast<const LiteralExpr*>(expr);
            const EvaluatedType type = lit->getType();
            const bool isList = type == EvaluatedType::List;
            if (!isList) {
                break;
            }

            const Literal* literal = lit->getLiteral();
            const auto* list = static_cast<const ListLiteral*>(literal);

            for (Expr* item : list->items()) {
                expandExpr(item);
            }
        }
        break;

        case Expr::Kind::INDEX:
            // Index ops are added for eval in guarded call to @ref needsEvaluation
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            // Is not aggregate: guaranteed by guard of @ref
            // isEvaluationBlocker
            const auto* func = static_cast<const FunctionInvocationExpr*>(expr);
            const FunctionInvocation* invok = func->getFunctionInvocation();
            const ExprChain* args = invok->getArguments();
            for (Expr* arg : *args) {
                _frontier.push(arg);
            }
        }
        break;

        case Expr::Kind::PROPERTY: {
            auto* prop = static_cast<PropertyExpr*>(expr);

            // If we are reading from CSV, we get the value from the ExprEval...
            const bool isCSVRead = prop->isStringTableHeaderAccess();
            if (isCSVRead) {
                break; // CSV reads added for eval in guarded call to @ref needsEvaluation
            }

            // ... otherwise, regular property; get from cache or generate.
            fetchOrGenerateProperty(prop);
        }
        break;

        case Expr::Kind::SYMBOL:
            // Symbol should already exist as a result of previous nodes
        break;

        case Expr::Kind::STRING:
        case Expr::Kind::ENTITY_TYPES:
            // TODO: Add a GetEntityType node (currently unused)
        case Expr::Kind::PATH:
        case Expr::Kind::LIST:
            throwError(
                fmt::format("{} expressions not yet supported in RETURN statements.",
                            ExprKindDescription::value(expr->getKind())),
                expr);
        break;

        case Expr::Kind::_SIZE:
            throwError("Unknown expression type.", expr);
        break;
    }
}

void ReturnStmtGenerator::handleEvaluationBlocker(const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
        case Expr::Kind::UNARY:
        case Expr::Kind::STRING:
        case Expr::Kind::ENTITY_TYPES:
        case Expr::Kind::PROPERTY:
        case Expr::Kind::PATH:
        case Expr::Kind::SYMBOL:
        case Expr::Kind::LITERAL:
        case Expr::Kind::INDEX:
        case Expr::Kind::LIST:
            throwError("Tried to handle non-blocking expression as blocker.", expr);
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            const auto* func = static_cast<const FunctionInvocationExpr*>(expr);
            // Only aggregate functions are blockers
            if (!func->isAggregate()) {
                throwError("Tried to handle non-aggregate function as blocker.", expr);
            }

            if (!_aggrEvalNode) {
                _aggrEvalNode = _tree->create<AggregateEvalNode>();
            }

            // Add the function to be evaluated
            _aggrEvalNode->addFunc(func);

            // Add each argument to the frontier; they may or may not be blocking
            const FunctionInvocation* invok = func->getFunctionInvocation();
            for (Expr* arg : *invok->getArguments()) {
                _frontier.push(arg);
            }
        }
        break;

        case Expr::Kind::_SIZE:
            throwError("Tried to handle unknown expression as blocker.", expr);
        break;
    }
}

void ReturnStmtGenerator::fetchOrGenerateProperty(PropertyExpr* prop) {
    const VarDecl* entityVar = prop->getEntityVarDecl();
    const VarDecl* propVar = prop->getExprVarDecl();
    const std::string_view propName = prop->getPropName();

    if (!propVar) {
        throwError("Property had no variable declaration.", prop);
    }

    if (!entityVar) {
        throwError("Property had no enitity variable declation.", prop);
    }

    {
        // Check if a variable is produced by a processor such as @ref
        // PathExplorerProcessor which does not produce a single entity column

        const PlanGraphNode* producer = _vars->getProducer(entityVar);
        const PlanGraphOpcode code = producer->getOpcode();

        const bool isFromExplorer = code == PlanGraphOpcode::PATH_EXPLORER;
        if (isFromExplorer) {
            throwError("Returning properties from a variable lengthed path is not yet "
                       "supported.",
                       prop);
        }

        const bool isFromVar = code == PlanGraphOpcode::VAR;
        if (!isFromVar) {
            throwError("Could not return properties from variable with ambiguous source.",
                       prop);
        }
    }

    const auto* cached = _propCache.cacheOrRetrieve(entityVar, propVar, propName);
    if (cached) {
        if (!cached->_exprDecl) {
            throwError("Cached property had no expression variable.", prop);
        }
        // GetProperty is already present in the cache. Map the existing propExpr
        // to the current one
        prop->setExprVarDecl(cached->_exprDecl);
        return;
    }

    // Otherwise we need to fetch this property just for the return, add a node and update
    // the propExpr. Place the GetProperties immediately: this ensures all properties are
    // present for any expression evaluations that may use them.
    auto* getProps = _tree->newOut<GetPropertyWithNullNode>(_prevNode, propName);
    _prevNode = getProps;

    getProps->setExpr(prop);
    getProps->setEntityVarDecl(entityVar);
}

void ReturnStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
