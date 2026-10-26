#include "ExistsExpr.h"

#include "CypherAST.h"
#include "RegularQuery.h"

using namespace db;

ExistsExpr::ExistsExpr(RegularQuery* body)
    : Expr(Expr::Kind::EXISTS),
    _body(body)
{
}

ExistsExpr::~ExistsExpr() {
}

ExistsExpr* ExistsExpr::create(CypherAST* ast, RegularQuery* body) {
    ast->nestQuery(body);

    ExistsExpr* expr = new ExistsExpr(body);
    ast->addExpr(expr);

    return expr;
}
