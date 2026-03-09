#include "ExprEvalNode.h"

#include "Literal.h"
#include "decl/EvaluatedType.h"
#include "expr/Expr.h"
#include "expr/PropertyExpr.h"

#include "BioAssert.h"


using namespace db;

ExprEvalNode::ExprEvalNode()
    : PlanGraphNode(PlanGraphOpcode::EXPR_EVAL)
{
}

ExprEvalNode::~ExprEvalNode() {
}

// Unary and Binary expressions need evaluating.
// Literals that appear only on the RHS of a return, e.g. `RETURN 5`, need a
// column allocated for them, otherwise they will not exist. @ref addExpr where
// @ref item is a literal will allocate this column.
// Wildcard (*) is of type LITERAL, but does not need evaluating.
bool ExprEvalNode::needsEvaluation(const Expr* expr) {
    const Expr::Kind exprKind = expr->getKind();

    const bool operatorType = exprKind == Expr::Kind::BINARY
                           || exprKind == Expr::Kind::UNARY
                           || exprKind == Expr::Kind::INDEX;

    if (operatorType) {
        return true;
    }

    const bool maybeCSVFieldAccess = exprKind == Expr::Kind::PROPERTY;
    if (maybeCSVFieldAccess) {
        const auto* propExpr = dynamic_cast<const PropertyExpr*>(expr);
        bioassert(propExpr, "Failed to cast PropertyExpr for evalutation.");
        const bool needsEval = propExpr->isStringTableHeaderAccess();
        if (needsEval) {
            return true;
        }
    }

    const bool isLiteral = exprKind == Expr::Kind::LITERAL;
    const bool isWildcard = expr->getType() == EvaluatedType::Wildcard;

    return isLiteral && !isWildcard;
}
