#include "CollectSubqueryExpr.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"

using namespace db;

CollectSubqueryExpr::CollectSubqueryExpr(const Branches& branches)
    : Expr(Expr::Kind::COLLECT_SUBQUERY),
    _branches(branches)
{
}

CollectSubqueryExpr::~CollectSubqueryExpr() {
}

CollectSubqueryExpr* CollectSubqueryExpr::create(CypherAST* ast, const Branches& branches) {
    for (const UnionQuery::Branch& branch : branches) {
        ast->nestQuery(branch._query);
    }

    CollectSubqueryExpr* expr = new CollectSubqueryExpr(branches);
    ast->addExpr(expr);

    return expr;
}
