#include "UnaryExpr.h"

#include "CypherAST.h"

using namespace db::v2;

UnaryExpr::~UnaryExpr() {
}

UnaryExpr* UnaryExpr::create(CypherAST* ast, UnaryOperator op, Expr* subExpr) {
    UnaryExpr* expr = new UnaryExpr(op, subExpr);
    ast->addExpr(expr);
    return expr;
}
