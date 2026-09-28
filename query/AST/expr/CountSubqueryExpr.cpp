#include "CountSubqueryExpr.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"

using namespace db;

CountSubqueryExpr::CountSubqueryExpr(const Branches& branches)
    : Expr(Expr::Kind::COUNT_SUBQUERY),
    _branches(branches)
{
}

CountSubqueryExpr::~CountSubqueryExpr() {
}

CountSubqueryExpr* CountSubqueryExpr::create(CypherAST* ast, const Branches& branches) {
    for (const UnionQuery::Branch& branch : branches) {
        ast->nestQuery(branch._query);
    }

    CountSubqueryExpr* expr = new CountSubqueryExpr(branches);
    ast->addExpr(expr);

    return expr;
}
