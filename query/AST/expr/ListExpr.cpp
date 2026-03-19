#include "ListExpr.h"

#include "CypherAST.h"

using namespace db;

ListExpr::ListExpr()
    : Expr(Expr::Kind::LIST)
{
}

ListExpr::~ListExpr() {
}

void ListExpr::addItem(Expr* expr) {
    _elements.push_back(expr);
}

ListExpr* ListExpr::create(CypherAST* ast) {
    ListExpr* expr = new ListExpr();
    ast->addExpr(expr);

    return expr;
}
