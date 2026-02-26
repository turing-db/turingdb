#include "ReturnStmtGenerator.h"

#include "CypherAST.h"
#include "ExprDependencies.h"
#include "PlanGraph.h"
#include "Projection.h"

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

#include "DiagnosticsManager.h"

using namespace db;

ReturnStmtGenerator::ReturnStmtGenerator(const CypherAST* ast,
                                         const ReturnStmt* rtnStmt,
                                         PlanGraph* tree,
                                         PlanGraphNode* prevNode,
                                         PlanGraphVariables* vars,
                                         GetPropertyCache& propCache,
                                         GetEntityTypeCache& entCache)
    : _ast(ast),
    _stmt(rtnStmt),
    _tree(tree),
    _prevNode(prevNode),
    _variables(vars),
    _propCache(propCache),
    _entCache(entCache)
{
}

void ReturnStmtGenerator::prepare() {
    _aggrEvalNode = _tree->create<AggregateEvalNode>();
    _funcEvalNode = _tree->create<FuncEvalNode>();
    _exprEvalNode = _tree->create<ExprEvalNode>();
    _proj = _stmt->getProjection();
    bioassert(_proj, "Failed to get projection for RETURN statement.");
}

PlanGraphNode* ReturnStmtGenerator::generateReturnStmt() {
    prepare();

    if (_proj->isDistinct()) {
        throwError("DISTINCT not yet supported.", _stmt);
    }

    // If a return item is an expression, add the dependencies to be evaluated
    for (const Projection::ReturnItem& returnItem : _proj->items()) {
        Expr* const* exprPtr = std::get_if<Expr*>(&returnItem);
        if (exprPtr) {
            handleExprDependencies(*exprPtr);
        }
    }

    // If the projection has an ORDER BY, add expression dependencies to be evaluated
    if (_proj->hasOrderBy()) {
        const auto& projOrderItems = _proj->getOrderBy()->getItems();
        // Get dependencies that we require to order, e.g.
        // `MATCH (n) RETURN n ORDER BY n.name`
        // requires inserting a plan node to get n.name
        for (const OrderByItem* item : projOrderItems) {
            Expr* itemExpr = item->getExpr();
            bioassert(itemExpr, "OrderByItem had null expr.");

            handleExprDependencies(itemExpr);
        }
    }

    // Expression evaluation is the only node which does not require a previous input, for example
    // `RETURN 5`
    // has no previous input, but is valid.
    if (!_exprEvalNode->getExprs().empty()) {
        if (_prevNode) {
            _prevNode->connectOut(_exprEvalNode);
        }
        _prevNode = _exprEvalNode;
    }

    // Functions always require some previous node, even in the case of
    // `RETURN sqrt(5)`
    // the `5` must have been produced by the above ExprEvalNode, which sets
    // @ref _prevNode to non-null
    // NOTE: In case of a function which takes no arguments being supported, the above
    // becomes false.
    if (_prevNode && !_funcEvalNode->getFuncs().empty()) {
        _prevNode->connectOut(_funcEvalNode);
        _prevNode = _funcEvalNode;
    }

    // Aggregates always require some previous node, even in the case of
    // `RETURN COUNT(5)`
    // the `5` must have been produced by the above ExprEvalNode, which sets
    // @ref _prevNode to non-null
    // NOTE: In case of an aggregate which takes no arguments being supported, the above
    // becomes false.
    if (_prevNode && !_aggrEvalNode->getFuncs().empty()) {
        _prevNode->connectOut(_aggrEvalNode);
        _prevNode = _aggrEvalNode;
    }

    // ORDER BY, SKIP, LIMIT require a previous input, `LIMIT 10` is not a valid query,
    // but `MATCH (n) LIMIT 10` is (because it has SCAN NODES as a previous input), and so
    // is `RETURN 5 LIMIT 10` (it has EXPR EVAL as a previous input). Therefore, we can
    // only add thse projection properties if @ref prevNode is valid.
    if (_prevNode && _proj->hasOrderBy()) {
        // Any expression dependencies, e.g.
        // `MATCH (n) RETURN n.age ORDER BY n.age + 10`
        // where `n.age + 10` need be evaluated first, are handled at the entrypoint of
        // this function, meaning at this point, if there is an ORDER BY, all dependent
        // columns will be registered.
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

void ReturnStmtGenerator::handleExprDependencies(Expr* expr) {
    if (ExprEvalNode::needsEvaluation(expr)) {
        _exprEvalNode->addExpr(expr);
    }

    ExprDependencies deps;
    deps.genExprDependencies(*_variables, expr);

    for (const ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
        if (!_prevNode) {
            throwError(
                "Expression had dependencies, but no previous node to provide them.",
                expr);
        }

        if (auto* propExpr = dynamic_cast<PropertyExpr*>(dep._expr)) {
            const VarDecl* entityDecl = propExpr->getEntityVarDecl();
            const VarDecl* propExprDecl = propExpr->getExprVarDecl();

            if (!propExprDecl) [[unlikely]] {
                throwError(
                    "Property propExpression does not have an propExpression variable "
                    "declaration",
                    propExpr);
            }

            if (!entityDecl) [[unlikely]] {
                throwError(
                    "Property propExpression does not have an entity variable declaration",
                    propExpr);
            }

            const auto* cached = _propCache.cacheOrRetrieve(entityDecl, propExprDecl,
                                                            propExpr->getPropName());

            if (cached) {
                // GetProperty is already present in the cache. Map the existing propExpr to
                // the current one
                if (!cached->_exprDecl) [[unlikely]] {
                    throwError("GetProperty propExpression does not have an propExpression "
                               "variable declaration",
                               propExpr);
                }

                propExpr->setExprVarDecl(cached->_exprDecl);
                continue;
            }

            auto* n = _tree->newOut<GetPropertyWithNullNode>(_prevNode,
                                                             propExpr->getPropName());
            n->setExpr(propExpr);
            n->setEntityVarDecl(entityDecl);
            _prevNode = n;
        } else if (auto* entExpr = dynamic_cast<EntityTypeExpr*>(dep._expr)) {
            const VarDecl* entityDecl = entExpr->getEntityVarDecl();
            const VarDecl* exprDecl = entExpr->getExprVarDecl();

            if (!exprDecl) [[unlikely]] {
                throwError("Entity type expression does not have an expression variable "
                           "declaration",
                           expr);
            }

            if (!entityDecl) [[unlikely]] {
                throwError(
                    "Entity type expression does not have an entity variable declaration",
                    expr);
            }

            const auto* cached = _entCache.cacheOrRetrieve(entityDecl, exprDecl);

            if (cached) {
                // GetEntityType is already present in the cache. Map the existing expr to
                // the current one

                if (!cached->_exprDecl) [[unlikely]] {
                    throwError("GetEntityType expression does not have an expression "
                               "variable declaration",
                               expr);
                }

                expr->setExprVarDecl(cached->_exprDecl);
                continue;
            }

            auto* n = _tree->newOut<GetEntityTypeNode>(_prevNode);
            n->setExpr(expr);
            n->setEntityVarDecl(entityDecl);
            _prevNode = n;
        } else if (dynamic_cast<const SymbolExpr*>(dep._expr)) {
            // Symbol value should already be in a column in a block, no need to change
            // anything
        } else {
            throwError(
                "Expression dependency could not be handled in the predicate evaluation");
        }
    }

    // Functions may have expressions which need be evaluated prior to the functions
    // evaluation, e.g. COUNT(5 + 5).
    for (const ExprDependencies::FuncDependency& dep : deps.getFuncDeps()) {
        const FunctionInvocationExpr* funcExpr = dep._expr;
        const FunctionInvocation* funcInvok = funcExpr->getFunctionInvocation();
        const FunctionSignature* signature = funcInvok->getSignature();

        const ExprChain* arguments = funcInvok->getArguments();
        for (const Expr* argument : *arguments) {
            if (ExprEvalNode::needsEvaluation(argument)) {
                _exprEvalNode->addExpr(argument);
            }
        }

        if (signature->isAggregate()) {
            _aggrEvalNode->addFunc(dep._expr);
        } else {
            _funcEvalNode->addFunc(dep._expr);
        }
    }

    if (_proj->isAggregate() && !expr->isAggregate()) {
        const Expr::Kind kind = expr->getKind();

        if (kind != Expr::Kind::SYMBOL && kind != Expr::Kind::PROPERTY) {
            throwError(
                "Complex grouping keys are not supported yet. Only variables (e.g. n), "
                "or property expression (e.g. n.name) are allowed",
                _proj);
        }

        _aggrEvalNode->addGroupByKey(expr);
    }
}

void ReturnStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
