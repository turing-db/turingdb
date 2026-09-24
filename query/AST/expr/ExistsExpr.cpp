#include "ExistsExpr.h"

#include "CypherAST.h"
#include "SinglePartQuery.h"

using namespace db;

ExistsExpr::ExistsExpr(const Branches& branches)
    : Expr(Expr::Kind::EXISTS),
    _branches(branches)
{
}

ExistsExpr::~ExistsExpr() {
}

ExistsExpr* ExistsExpr::create(CypherAST* ast, const Branches& branches) {
    for (SinglePartQuery* branch : branches) {
        ast->nestQuery(branch);
    }

    ExistsExpr* expr = new ExistsExpr(branches);
    ast->addExpr(expr);

    return expr;
}
