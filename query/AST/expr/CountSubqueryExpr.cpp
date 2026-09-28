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

size_t CountSubqueryExpr::getDedupedBranchCount() const {
    size_t deduped = 0;

    for (size_t index = 1; index < _branches.size(); index++) {
        if (!_branches[index]._all) {
            deduped = index + 1;
        }
    }

    return deduped;
}

CountSubqueryExpr* CountSubqueryExpr::create(CypherAST* ast, const Branches& branches) {
    for (const UnionQuery::Branch& branch : branches) {
        ast->nestQuery(branch._query);
    }

    CountSubqueryExpr* expr = new CountSubqueryExpr(branches);
    ast->addExpr(expr);

    return expr;
}
