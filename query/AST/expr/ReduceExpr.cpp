#include "ReduceExpr.h"

#include "CypherAST.h"

using namespace db;

ReduceExpr::ReduceExpr(Symbol* accumulatorSymbol,
                       Expr* initialValue,
                       Symbol* itemSymbol,
                       Expr* source,
                       Expr* expression)
    : Expr(Expr::Kind::REDUCE),
    _accumulatorSymbol(accumulatorSymbol),
    _initialValue(initialValue),
    _itemSymbol(itemSymbol),
    _source(source),
    _expression(expression)
{
}

ReduceExpr::~ReduceExpr() {
}

ReduceExpr* ReduceExpr::create(CypherAST* ast,
                               Symbol* accumulatorSymbol,
                               Expr* initialValue,
                               Symbol* itemSymbol,
                               Expr* source,
                               Expr* expression) {
    ReduceExpr* expr = new ReduceExpr(accumulatorSymbol, initialValue, itemSymbol, source, expression);
    ast->addExpr(expr);

    return expr;
}
