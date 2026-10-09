#include "BinaryExpr.h"

#include "CypherAST.h"

using namespace db;

BinaryExpr::~BinaryExpr() {
}

BinaryExpr* BinaryExpr::create(CypherAST* ast,
                               BinaryOperator op,
                               Expr* lhs,
                               Expr* rhs) {
    BinaryExpr* expr = new BinaryExpr(op, lhs, rhs);
    ast->addExpr(expr);
    return expr;
}
