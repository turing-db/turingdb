#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Symbol;
class VarDecl;

// A Cypher allReduce: `allReduce(acc = init, x IN xs | f(acc, x), p(acc, x))`. The
// accumulator is folded over the source list as reduce folds it, and the predicate is
// tested after every step, over the accumulator that step computed and its element. The
// value is whether the predicate held at every step.
class AllReduceExpr : public Expr {
public:
    const Symbol* getAccumulatorSymbol() const { return _accumulatorSymbol; }
    Expr* getInitialValue() const { return _initialValue; }
    const Symbol* getItemSymbol() const { return _itemSymbol; }
    Expr* getSource() const { return _source; }
    Expr* getExpression() const { return _expression; }
    Expr* getPredicate() const { return _predicate; }
    const VarDecl* getAccumulatorDecl() const { return _accumulatorDecl; }
    const VarDecl* getItemDecl() const { return _itemDecl; }

    void setAccumulatorDecl(const VarDecl* decl) { _accumulatorDecl = decl; }
    void setItemDecl(const VarDecl* decl) { _itemDecl = decl; }

    static AllReduceExpr* create(CypherAST* ast,
                                 Symbol* accumulatorSymbol,
                                 Expr* initialValue,
                                 Symbol* itemSymbol,
                                 Expr* source,
                                 Expr* expression,
                                 Expr* predicate);

private:
    Symbol* _accumulatorSymbol {nullptr};
    Expr* _initialValue {nullptr};
    Symbol* _itemSymbol {nullptr};
    Expr* _source {nullptr};
    Expr* _expression {nullptr};
    Expr* _predicate {nullptr};
    const VarDecl* _accumulatorDecl {nullptr};
    const VarDecl* _itemDecl {nullptr};

    AllReduceExpr(Symbol* accumulatorSymbol,
                  Expr* initialValue,
                  Symbol* itemSymbol,
                  Expr* source,
                  Expr* expression,
                  Expr* predicate);
    ~AllReduceExpr() override;
};

}
