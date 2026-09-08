#include "CaseExpr.h"

#include "CypherAST.h"

using namespace db;

CaseExpr::CaseExpr()
    : Expr(Expr::Kind::CASE)
{
}

CaseExpr::~CaseExpr() {
}

void CaseExpr::addBranch(Expr* when, Expr* then) {
    _branches.push_back({._when = when, ._then = then});
}

CaseExpr* CaseExpr::create(CypherAST* ast) {
    CaseExpr* expr = new CaseExpr();
    ast->addExpr(expr);

    return expr;
}
