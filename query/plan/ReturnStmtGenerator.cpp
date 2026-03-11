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
#include "decl/VarDecl.h"
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
        _exprEvalNode = _tree->create<ExprEvalNode>();

        Expr* root = *exprPtr;
        // BFS from root; blocked by aggregates
        _frontier = std::queue<Expr*> {};
        _blockers = std::queue<Expr*> {};
        _frontier.push(root);

        while (!_frontier.empty() || !_blockers.empty()) {
            while (!_frontier.empty()) {
                Expr* front = _frontier.front();
                _frontier.pop();

                // Evaluation blockers need be evaluated on their own level
                if (isEvaluationBlocker(front)) {
                    _blockers.push(front);
                    continue;
                }

                if (ExprEvalNode::needsEvaluation(front)) {
                    _exprEvalNode->addExpr(front);
                }

                // Adds all to @ref _frontier that we can
                treeWalkExpr(front);
            }

            if (!_exprEvalNode->getExprs().empty()) {
                evalSteps.emplace_back(_exprEvalNode);
                _exprEvalNode = _tree->create<ExprEvalNode>();
            }

            // Repopulate exploration queue with blockers
            _aggrEvalNode = _tree->create<AggregateEvalNode>();
            while (!_blockers.empty()) {
                const Expr* blocker = _blockers.front();
                _blockers.pop();

                handleEvaluationBlocker(blocker);
            }

            if (!_aggrEvalNode->getFuncs().empty()) {
                evalSteps.emplace_back(_aggrEvalNode);
                _aggrEvalNode = _tree->create<AggregateEvalNode>();
            }
        }
    }

    if (!evalSteps.empty()) {
        const auto addStepToPlan = [this](auto&& evalNode) {
            if (_prevNode) {
                _prevNode->connectOut(evalNode);
            }
            _prevNode = evalNode;
        };

        {
            const EvaluationStep& firstEvalStep = evalSteps.back();
            std::visit(addStepToPlan, firstEvalStep);
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

bool ReturnStmtGenerator::isEvaluationBlocker(const Expr* expr) {
    const auto* function = dynamic_cast<const FunctionInvocationExpr*>(expr);
    const bool isAggregate = function && function->isAggregate();

    const bool isBlocker = isAggregate /* || other blockers */;

    return isBlocker;
}

void ReturnStmtGenerator::treeWalkExpr(Expr* expr) {
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

        case Expr::Kind::LITERAL:
            // Literal is generated in guarded call to @ref needsEvaluation
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
            const VarDecl* entityVar = prop->getEntityVarDecl();
            const VarDecl* propVar = prop->getExprVarDecl();
            const std::string_view propName = prop->getPropName();

            if (!propVar) {
                throwError("Property had no variable declaration.", prop);
            }

            if (!entityVar) {
                throwError("Property had no enitity variable declation.", prop);
            }

            const auto* cached = _propCache.cacheOrRetrieve(entityVar, propVar, propName);
            if (cached) {
                // GetProperty is already present in the cache. Map the existing propExpr
                // to the current one
                if (!cached->_exprDecl) {
                    throwError("Cached property had no expression variable.", prop);
                }

                prop->setExprVarDecl(cached->_exprDecl);
                break;
            }
            // TODO: Add a get properties
        }
        break;


        case Expr::Kind::SYMBOL:
            // Symbol should already exist as a result of previous nodes
        break;

        case Expr::Kind::STRING:
        case Expr::Kind::ENTITY_TYPES:
            // TODO: Add a GetEntityType node (currently unused)
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

void ReturnStmtGenerator::throwError(std::string_view msg, const void* obj) const {
    std::string errorStr;
    _ast->getDiagnosticsManager()->createErrorString(msg, obj, errorStr);
    throw PlannerException(std::move(errorStr));
}
