#include "UnaryExpr.h"

#include "CypherAST.h"

using namespace db;

UnaryExpr::~UnaryExpr() {
}

UnaryExpr* UnaryExpr::create(CypherAST* ast, UnaryOperator op, Expr* subExpr) {
    UnaryExpr* expr = new UnaryExpr(op, subExpr);
    ast->addExpr(expr);
    return expr;
}
