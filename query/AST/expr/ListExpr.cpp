#include "ListExpr.h"

#include "CypherAST.h"

using namespace db;

void ListExpr::addItem(Expr* expr) {
    _elements.push_back(expr);
}

ListExpr::~ListExpr() {
}

ListExpr* ListExpr::create(CypherAST* ast) {
    ListExpr* expr = new ListExpr();
    ast->addExpr(expr);

    return expr;
}
