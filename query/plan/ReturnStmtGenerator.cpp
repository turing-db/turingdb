#include "ReturnStmtGenerator.h"

#include <queue>
#include <variant>
#include <vector>

#include <range/v3/view/drop.hpp>
#include <range/v3/view/reverse.hpp>

#include "CypherAST.h"
#include "ExprDependencies.h"
#include "FunctionInvocation.h"
#include "PlanGraph.h"
#include "Projection.h"

#include "QualifiedName.h"
#include "Symbol.h"
#include "expr/BinaryExpr.h"
#include "expr/Expr.h"
#include "expr/LiteralExpr.h"
#include "expr/UnaryExpr.h"
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
    _exprEvalNode = _tree->create<ExprEvalNode>();
    _proj = _stmt->getProjection();
    bioassert(_proj, "Failed to get projection for RETURN statement.");
}

PlanGraphNode* ReturnStmtGenerator::generateReturnStmt() {
    prepare();

    if (_proj->isDistinct()) {
        throwError("DISTINCT not yet supported.", _stmt);
    }

    using EvaluationStep = std::variant<ExprEvalNode*, AggregateEvalNode*>;

    std::vector<EvaluationStep> evalSteps;

    for (const Projection::ReturnItem& returnItem : _proj->items()) {
        Expr* const* exprPtr = std::get_if<Expr*>(&returnItem);
        if (!exprPtr) {
            continue;
        }

        // TODO: Can we reuse a single eval node for multiple independent statements?
        ExprEvalNode* exprEval = _tree->create<ExprEvalNode>();

        const Expr* root = *exprPtr;
        // BFS from root; blocked by aggregates
        _frontier = std::queue<const Expr*> {};
        _blockers = std::queue<const Expr*> {};
        _frontier.push(root);

        while (!_frontier.empty() || !_blockers.empty()) {
            while (!_frontier.empty()) {
                const Expr* front = _frontier.front();
                _frontier.pop();

                // Evaluation blockers need be evaluated on their own level
                if (isEvaluationBlocker(front)) {
                    _blockers.push(front);
                    continue;
                }

                if (ExprEvalNode::needsEvaluation(front)) {
                    exprEval->addExpr(front);
                }

                switch (front->getKind()) {
                    case Expr::Kind::BINARY: {
                        const auto* bin = static_cast<const BinaryExpr*>(front);
                        const Expr* lhs = bin->getLHS();
                        const Expr* rhs = bin->getRHS();
                        _frontier.push(lhs);
                        _frontier.push(rhs);
                    } break;

                    case Expr::Kind::UNARY: {
                        const auto* unary = static_cast<const UnaryExpr*>(front);
                        const Expr* subExpr = unary->getSubExpr();
                        _frontier.push(subExpr);
                    } break;

                    case Expr::Kind::LITERAL:
                        // Literal is generated in guarded call to @ref needsEvaluation
                        break;

                    case Expr::Kind::FUNCTION_INVOCATION: {
                        // Is not aggregate: guaranteed by guard of @ref
                        // isEvaluationBlocker
                        const auto* func =
                            static_cast<const FunctionInvocationExpr*>(front);
                        const FunctionInvocation* invok = func->getFunctionInvocation();
                        const ExprChain* args = invok->getArguments();
                        for (const Expr* arg : *args) {
                            _frontier.push(arg);
                        }
                    } break;

                    case Expr::Kind::SYMBOL:
                        // Symbol should already exist as a result of previous nodes
                        break;

                    case Expr::Kind::STRING:
                    case Expr::Kind::ENTITY_TYPES:
                    case Expr::Kind::PROPERTY:
                    case Expr::Kind::PATH:
                    case Expr::Kind::INDEX:
                    case Expr::Kind::LIST:
                        throwError(
                            fmt::format(
                                "{} expressions not yet supported in RETURN statements.",
                                ExprKindDescription::value(front->getKind())),
                            front);
                        break;

                    case Expr::Kind::_SIZE:
                        throwError("Unknown expression type.", front);
                        break;
                }
            }

            if (!exprEval->getExprs().empty()) {
                evalSteps.emplace_back(exprEval);
                exprEval = _tree->create<ExprEvalNode>();
            }

            // Repopulate exploration queue with blockers
            while (!_blockers.empty()) {
                const Expr* blocker = _blockers.front();
                _blockers.pop();

                // FIXME: Handle non-COUNT() blockers
                const auto* aggr = dynamic_cast<const FunctionInvocationExpr*>(blocker);
                bioassert(aggr, "Failed to cast blocking evaluation.");
                bioassert(aggr->isAggregate(), "Blocker was not aggregate");

                AggregateEvalNode* aggrEval = _tree->create<AggregateEvalNode>();
                aggrEval->addFunc(aggr);
                evalSteps.emplace_back(aggrEval);

                const auto* invok = aggr->getFunctionInvocation();
                for (const Expr* arg : *invok->getArguments()) {
                    _frontier.push(arg);
                }
            }
        }
    }

    if (!evalSteps.empty()) {
        const auto addStepToPlan = [this](auto&& evalNode) {
            _prevNode->connectOut(evalNode);
            _prevNode = evalNode;
        };

        {
            const EvaluationStep& firstEvalStep = evalSteps.back();
            if (_prevNode) {
                std::visit(addStepToPlan, firstEvalStep);
            } else {
                std::visit([this](auto&& fstStep) { _prevNode = fstStep; },
                           firstEvalStep);
            }
        }

        for (const EvaluationStep& evalStep : rv::reverse(evalSteps) | rv::drop(1)) {
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

bool ReturnStmtGenerator::isEvaluationBlocker(const Expr* expr) {
    const auto* function = dynamic_cast<const FunctionInvocationExpr*>(expr);
    const bool isAggregate = function && function->isAggregate();

    const bool isBlocker = isAggregate /* || other blockers */;

    return isBlocker;
}

void ReturnStmtGenerator::treeWalkExpr(const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY: {
            const auto* bin = static_cast<const BinaryExpr*>(expr);
            const Expr* lhs = bin->getLHS();
            const Expr* rhs = bin->getRHS();
            _frontier.push(lhs);
            _frontier.push(rhs);
        }
        break;

        case Expr::Kind::UNARY: {
            const auto* unary = static_cast<const UnaryExpr*>(expr);
            const Expr* subExpr = unary->getSubExpr();
            _frontier.push(subExpr);
        }
        break;

        case Expr::Kind::LITERAL:
            // Literal is generated in guarded call to @ref needsEvaluation
        break;

        case Expr::Kind::FUNCTION_INVOCATION: {
            // Is not aggregate: guaranteed by guard of @ref
            // isEvaluationBlocker
            const auto* func = static_cast<const FunctionInvocationExpr*>(expr);
            const FunctionInvocation* invok = func->getFunctionInvocation();
            const ExprChain* args = invok->getArguments();
            for (const Expr* arg : *args) {
                _frontier.push(arg);
            }
        }
        break;

        case Expr::Kind::SYMBOL:
            // Symbol should already exist as a result of previous nodes
        break;

        case Expr::Kind::STRING:
        case Expr::Kind::ENTITY_TYPES:
        case Expr::Kind::PROPERTY:
        case Expr::Kind::PATH:
        case Expr::Kind::INDEX:
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

void ReturnStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
