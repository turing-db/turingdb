#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Symbol;
class VarDecl;

// A Cypher reduce: `reduce(acc = init, x IN xs | f(acc, x))`. The accumulator starts as the
// initial value and the expression replaces it once per element of the source list, in
// order; the value is the accumulator once every element has been seen.
class ReduceExpr : public Expr {
public:
    const Symbol* getAccumulatorSymbol() const { return _accumulatorSymbol; }
    Expr* getInitialValue() const { return _initialValue; }
    const Symbol* getItemSymbol() const { return _itemSymbol; }
    Expr* getSource() const { return _source; }
    Expr* getExpression() const { return _expression; }
    const VarDecl* getAccumulatorDecl() const { return _accumulatorDecl; }
    const VarDecl* getItemDecl() const { return _itemDecl; }

    void setAccumulatorDecl(const VarDecl* decl) { _accumulatorDecl = decl; }
    void setItemDecl(const VarDecl* decl) { _itemDecl = decl; }

    static ReduceExpr* create(CypherAST* ast,
                              Symbol* accumulatorSymbol,
                              Expr* initialValue,
                              Symbol* itemSymbol,
                              Expr* source,
                              Expr* expression);

private:
    Symbol* _accumulatorSymbol {nullptr};
    Expr* _initialValue {nullptr};
    Symbol* _itemSymbol {nullptr};
    Expr* _source {nullptr};
    Expr* _expression {nullptr};
    const VarDecl* _accumulatorDecl {nullptr};
    const VarDecl* _itemDecl {nullptr};

    ReduceExpr(Symbol* accumulatorSymbol,
               Expr* initialValue,
               Symbol* itemSymbol,
               Expr* source,
               Expr* expression);
    ~ReduceExpr() override;
};

}
