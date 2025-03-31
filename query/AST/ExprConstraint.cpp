#include "ExprConstraint.h"

#include "ASTContext.h"

using namespace db;

ExprConstraint::ExprConstraint()
{
}

ExprConstraint::~ExprConstraint() {
}

ExprConstraint* ExprConstraint::create(ASTContext* ctxt) {
    ExprConstraint* constr = new ExprConstraint();
    ctxt->addExprConstraint(constr);
    return constr;
}

void ExprConstraint::addExpr(BinExpr* expr){
    _expressions.push_back(expr);
}
