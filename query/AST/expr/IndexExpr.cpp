#include "IndexExpr.h"

#include "CypherAST.h"

using namespace db;

IndexExpr::IndexExpr(Expr* base, Expr* indexExpr)
    : Expr(Kind::INDEX),
    _base(base),
    _indexExpr(indexExpr)
{
}

IndexExpr::~IndexExpr() = default;

IndexExpr* IndexExpr::create(CypherAST* ast, Expr* base, Expr* indexExpr) {
    IndexExpr* expr = new IndexExpr(base, indexExpr);
    ast->addExpr(expr);
    return expr;
}
