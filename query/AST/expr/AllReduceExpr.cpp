#include "AllReduceExpr.h"

#include "CypherAST.h"

using namespace db;

AllReduceExpr::AllReduceExpr(Symbol* accumulatorSymbol,
                             Expr* initialValue,
                             Symbol* itemSymbol,
                             Expr* source,
                             Expr* expression,
                             Expr* predicate)
    : Expr(Expr::Kind::ALL_REDUCE),
    _accumulatorSymbol(accumulatorSymbol),
    _initialValue(initialValue),
    _itemSymbol(itemSymbol),
    _source(source),
    _expression(expression),
    _predicate(predicate)
{
}

AllReduceExpr::~AllReduceExpr() {
}

AllReduceExpr* AllReduceExpr::create(CypherAST* ast,
                                     Symbol* accumulatorSymbol,
                                     Expr* initialValue,
                                     Symbol* itemSymbol,
                                     Expr* source,
                                     Expr* expression,
                                     Expr* predicate) {
    AllReduceExpr* expr = new AllReduceExpr(accumulatorSymbol, initialValue, itemSymbol, source, expression, predicate);
    ast->addExpr(expr);

    return expr;
}
