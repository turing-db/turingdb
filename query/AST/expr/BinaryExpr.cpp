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

BinaryExpr* BinaryExpr::createComparisonChain(CypherAST* ast, Expr* lhs, Expr* rhs) {
    BinaryExpr* const chain = create(ast, BinaryOperator::And, lhs, rhs);
    chain->_comparisonChain = true;

    return chain;
}
