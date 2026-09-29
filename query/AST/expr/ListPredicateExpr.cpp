#include "ListPredicateExpr.h"

#include "CypherAST.h"

using namespace db;

ListPredicateExpr::ListPredicateExpr(Quantifier quantifier, ListComprehensionExpr* comprehension)
    : Expr(Expr::Kind::LIST_PREDICATE),
    _quantifier(quantifier),
    _comprehension(comprehension)
{
}

ListPredicateExpr::~ListPredicateExpr() {
}

ListPredicateExpr* ListPredicateExpr::create(CypherAST* ast,
                                             Quantifier quantifier,
                                             ListComprehensionExpr* comprehension) {
    ListPredicateExpr* expr = new ListPredicateExpr(quantifier, comprehension);
    ast->addExpr(expr);

    return expr;
}
