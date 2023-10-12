#include "Expr.h"

#include "ASTContext.h"

using namespace db;

Expr::~Expr() {
}

void Expr::postCreate(ASTContext* ctxt) {
    ctxt->addExpr(this);
}

// VarExpr
VarExpr::VarExpr(const std::string& varName)
    : _varName(varName)
{
}

VarExpr::~VarExpr() {
}

VarExpr* VarExpr::create(ASTContext* ctxt, const std::string& varName) {
    VarExpr* expr = new VarExpr(varName);
    expr->postCreate(ctxt);
    return expr;
}
