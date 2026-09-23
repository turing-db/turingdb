#include "ExistsExpr.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"

using namespace db;

ExistsExpr::ExistsExpr(SinglePartQuery* body)
    : Expr(Expr::Kind::EXISTS),
    _body(body)
{
}

ExistsExpr::~ExistsExpr() {
}

ExistsExpr* ExistsExpr::create(CypherAST* ast, SinglePartQuery* body) {
    ast->nestQuery(body);

    ExistsExpr* expr = new ExistsExpr(body);
    ast->addExpr(expr);

    return expr;
}
