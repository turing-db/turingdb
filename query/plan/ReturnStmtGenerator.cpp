#include "ReturnStmtGenerator.h"

#include "ExprDependencies.h"
#include "PlanGraph.h"

#include "Projection.h"

#include "nodes/ProduceResultsNode.h"
#include "stmt/Limit.h"
#include "stmt/OrderBy.h"
#include "stmt/OrderByItem.h"
#include "stmt/ReturnStmt.h"
#include "stmt/Skip.h"

#include "nodes/AggregateEvalNode.h"
#include "nodes/FuncEvalNode.h"
#include "nodes/GetEntityTypeNode.h"
#include "nodes/GetPropertyWithNullNode.h"
#include "nodes/LimitNode.h"
#include "nodes/OrderByNode.h"
#include "nodes/SkipNode.h"

using namespace db;

ReturnStmtGenerator::ReturnStmtGenerator(const ReturnStmt* rtnStmt,
                                         PlanGraph* tree,
                                         PlanGraphNode* prevNode,
                                         PlanGraphVariables* vars,
                                         GetPropertyCache& propCache,
                                         GetEntityTypeCache& entCache)
    : _stmt(rtnStmt),
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
    _proj = _stmt->getProjection();
}

PlanGraphNode* ReturnStmtGenerator::generateReturnStmt() {
    if (_proj->isDistinct()) {
        throwError("DISTINCT not supported", _stmt);
    }

    for (const Projection::ReturnItem& returnItem : _proj->items()) {
        Expr* const* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        handleExprDependencies(*exprPtr);
    }

    if (!_funcEvalNode->getFuncs().empty()) {
        _prevNode->connectOut(_funcEvalNode);
        _prevNode = _funcEvalNode;
    }

    if (!_aggrEvalNode->getFuncs().empty()) {
        _prevNode->connectOut(_aggrEvalNode);
        _prevNode = _aggrEvalNode;
    }

    if (_proj->hasOrderBy()) {
        auto* orderBy = _tree->newOut<OrderByNode>(_prevNode);
        const auto& projOrderItems = _proj->getOrderBy()->getItems();

        // Get dependencies that we require to order, e.g.
        // MATCH (n) RETURN n ORDER BY n.name
        // requires inserting a plan node to get n.name
        for (const OrderByItem* item : projOrderItems) {
            Expr* itemExpr = item->getExpr();
            handleExprDependencies(itemExpr);
        }
        
        orderBy->setItems(_proj->getOrderBy()->getItems());
        _prevNode = orderBy;
    }

    if (_proj->hasSkip()) {
        auto* skip = _tree->newOut<SkipNode>(_prevNode);
        skip->setExpr(_proj->getSkip()->getExpr());
        _prevNode = skip;
    }

    if (_proj->hasLimit()) {
        auto* limit = _tree->newOut<LimitNode>(_prevNode);
        limit->setExpr(_proj->getLimit()->getExpr());
        _prevNode = limit;
    }

    auto* results = _tree->newOut<ProduceResultsNode>(_prevNode);
    results->setProjection(_proj);

    return results;
}

void ReturnStmtGenerator::handleExprDependencies(Expr* expr) {
    ExprDependencies deps;
    deps.genExprDependencies(*_variables, expr);

    for (const ExprDependencies::VarDependency& dep : deps.getVarDeps()) {
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

    for (const ExprDependencies::FuncDependency& dep : deps.getFuncDeps()) {
        const FunctionInvocation* func = dep._expr->getFunctionInvocation();
        const FunctionSignature* signature = func->getSignature();

        if (signature->_isAggregate) {
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
