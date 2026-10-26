#include "CollectSubqueryExpr.h"

#include "CypherAST.h"
#include "RegularQuery.h"

using namespace db;

CollectSubqueryExpr::CollectSubqueryExpr(RegularQuery* body)
    : Expr(Expr::Kind::COLLECT_SUBQUERY),
    _body(body)
{
}

CollectSubqueryExpr::~CollectSubqueryExpr() {
}

CollectSubqueryExpr* CollectSubqueryExpr::create(CypherAST* ast, RegularQuery* body) {
    ast->nestQuery(body);

    CollectSubqueryExpr* expr = new CollectSubqueryExpr(body);
    ast->addExpr(expr);

    return expr;
}
