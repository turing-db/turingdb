#include "TypeConstraint.h"

#include "ASTContext.h"

using namespace db;

TypeConstraint::TypeConstraint()
{
}

TypeConstraint::~TypeConstraint() {
}

void TypeConstraint::addType(VarExpr* typeName) {
    _types.push_back(typeName);
}

TypeConstraint* TypeConstraint::create(ASTContext* ctxt) {
    TypeConstraint* constr = new TypeConstraint();
    ctxt->addTypeConstraint(constr);
    return constr;
}
