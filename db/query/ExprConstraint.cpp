#include "ExprConstraint.h"

#include "ASTContext.h"

using namespace db;

ExprConstraint::ExprConstraint(Expr* expr)
    : _expr(expr)
{
}

ExprConstraint::~ExprConstraint() {
}

ExprConstraint* ExprConstraint::create(ASTContext* ctxt, Expr* expr) {
    ExprConstraint* constr = new ExprConstraint(expr);
    ctxt->addExprConstraint(constr);
    return constr;
}
