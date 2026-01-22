#include "ListExpr.h"

#include "CypherAST.h"

using namespace db;

ListExpr::~ListExpr() {
}

ListExpr* ListExpr::create(CypherAST* ast) {
    ListExpr* expr = new ListExpr();
    ast->addExpr(expr);

    return expr;
}
