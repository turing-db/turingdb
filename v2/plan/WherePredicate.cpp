#include "WherePredicate.h"

#include "PlanGraphVariables.h"
#include "PlannerException.h"

#include "expr/BinaryExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/StringExpr.h"
#include "expr/NodeLabelExpr.h"
#include "expr/PropertyExpr.h"

#include "nodes/FilterNode.h"

using namespace db::v2;

void WherePredicate::genExprDependencies(const PlanGraphVariables& variables, const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            genExprDependencies(variables, static_cast<const BinaryExpr*>(expr));
            break;

        case Expr::Kind::UNARY:
            genExprDependencies(variables, static_cast<const UnaryExpr*>(expr));
            break;

        case Expr::Kind::STRING:
            genExprDependencies(variables, static_cast<const StringExpr*>(expr));
            break;

        case Expr::Kind::NODE_LABEL:
            genExprDependencies(variables, static_cast<const NodeLabelExpr*>(expr));
            break;

        case Expr::Kind::PROPERTY:
            genExprDependencies(variables, static_cast<const PropertyExpr*>(expr));
            break;

        case Expr::Kind::PATH:
            // throwError("Path expression not supported yet", expr);
            // TODO Find a way to get access to throwError
            throw PlannerException("Path expression not supported yet");
            break;

        case Expr::Kind::SYMBOL:
            // throwError("Symbol expression not supported yet", expr);
            // TODO Find a way to get access to throwError
            throw PlannerException("Symbol expression not supported yet");
            break;

        case Expr::Kind::LITERAL:
            fmt::print("Reached the end of the expression. LITERAL\n");
            break;
    }
}

void WherePredicate::genExprDependencies(const PlanGraphVariables& variables, const BinaryExpr* expr) {
    genExprDependencies(variables, expr->getLHS());
    genExprDependencies(variables, expr->getRHS());
}

void WherePredicate::genExprDependencies(const PlanGraphVariables& variables, const UnaryExpr* expr) {
    genExprDependencies(variables, expr->getSubExpr());
}

void WherePredicate::genExprDependencies(const PlanGraphVariables& variables, const StringExpr* expr) {
    genExprDependencies(variables, expr->getLHS());
    genExprDependencies(variables, expr->getRHS());
}

void WherePredicate::genExprDependencies(const PlanGraphVariables& variables, const NodeLabelExpr* expr) {
    _dependencies.addDependency(variables.getVarNode(expr->getDecl()), expr->labelSet());
}

void WherePredicate::genExprDependencies(const PlanGraphVariables& variables, const PropertyExpr* expr) {
    _dependencies.addDependency(variables.getVarNode(expr->getDecl()), expr->getPropertyType());
}
