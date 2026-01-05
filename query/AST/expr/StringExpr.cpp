#include "StringExpr.h"

#include "CypherAST.h"

using namespace db;

StringExpr::~StringExpr() {
}

StringExpr* StringExpr::create(CypherAST* ast, StringOperator op, Expr* lhs, Expr* rhs) {
    StringExpr* expr = new StringExpr(op, lhs, rhs);
    ast->addExpr(expr);
    return expr;
}
