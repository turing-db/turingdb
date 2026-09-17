#include "ListComprehensionExpr.h"

#include "CypherAST.h"

using namespace db;

ListComprehensionExpr::ListComprehensionExpr(Symbol* symbol, Expr* source)
    : Expr(Expr::Kind::LIST_COMPREHENSION),
    _symbol(symbol),
    _source(source)
{
}

ListComprehensionExpr::~ListComprehensionExpr() {
}

ListComprehensionExpr* ListComprehensionExpr::create(CypherAST* ast, Symbol* symbol, Expr* source) {
    ListComprehensionExpr* expr = new ListComprehensionExpr(symbol, source);
    ast->addExpr(expr);

    return expr;
}
