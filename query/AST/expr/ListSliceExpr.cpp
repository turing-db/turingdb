#include "ListSliceExpr.h"

#include "CypherAST.h"

using namespace db;

ListSliceExpr::ListSliceExpr(Expr* base, Expr* from, Expr* to)
    : Expr(Kind::LIST_SLICE),
    _base(base),
    _from(from),
    _to(to)
{
}

ListSliceExpr::~ListSliceExpr() {
}

ListSliceExpr* ListSliceExpr::create(CypherAST* ast, Expr* base, Expr* from, Expr* to) {
    ListSliceExpr* expr = new ListSliceExpr(base, from, to);
    ast->addExpr(expr);

    return expr;
}
