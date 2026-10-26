#include "CountSubqueryExpr.h"

#include "CypherAST.h"
#include "RegularQuery.h"

using namespace db;

CountSubqueryExpr::CountSubqueryExpr(RegularQuery* body)
    : Expr(Expr::Kind::COUNT_SUBQUERY),
    _body(body)
{
}

CountSubqueryExpr::~CountSubqueryExpr() {
}

CountSubqueryExpr* CountSubqueryExpr::create(CypherAST* ast, RegularQuery* body) {
    ast->nestQuery(body);

    CountSubqueryExpr* expr = new CountSubqueryExpr(body);
    ast->addExpr(expr);

    return expr;
}
