#include "WherePredicate.h"

#include "PlannerException.h"
#include "expr/BinaryExpr.h"
#include "expr/UnaryExpr.h"
#include "expr/StringExpr.h"
#include "expr/NodeLabelExpr.h"
#include "expr/PropertyExpr.h"

using namespace db::v2;

void WherePredicate::genExprDependencies(const Expr* expr) {
    switch (expr->getKind()) {
        case Expr::Kind::BINARY:
            genExprDependencies(static_cast<const BinaryExpr*>(expr));
            break;

        case Expr::Kind::UNARY:
            genExprDependencies(static_cast<const UnaryExpr*>(expr));
            break;

        case Expr::Kind::STRING:
            genExprDependencies(static_cast<const StringExpr*>(expr));
            break;

        case Expr::Kind::NODE_LABEL:
            genExprDependencies( static_cast<const NodeLabelExpr*>(expr));
            break;

        case Expr::Kind::PROPERTY:
            genExprDependencies( static_cast<const PropertyExpr*>(expr));
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

void WherePredicate::genExprDependencies(const BinaryExpr* expr) {
    genExprDependencies(expr->getLHS());
    genExprDependencies(expr->getRHS());
}

void WherePredicate::genExprDependencies(const UnaryExpr* expr) {
    genExprDependencies(expr->getSubExpr());
}

void WherePredicate::genExprDependencies(const StringExpr* expr) {
    genExprDependencies(expr->getLHS());
    genExprDependencies(expr->getRHS());
}

void WherePredicate::genExprDependencies(const NodeLabelExpr* expr) {
    _dependencies.addDependency(expr->getDecl(), expr->labelSet());
}

void WherePredicate::genExprDependencies(const PropertyExpr* expr) {
    _dependencies.addDependency(expr->getDecl(), expr->getPropertyType());
}

