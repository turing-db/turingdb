#pragma once

#include "Expr.h"

namespace db {

class CypherAST;

// A Cypher list slice: `xs[1..3]`, `xs[..3]`, `xs[1..]`. The elements from the lower bound
// up to but not including the upper one, each bound counting from the end where it is
// negative and clamped to the list where it runs past it. A bound the query leaves out is
// null here: the slice then runs from the start, or to the end.
class ListSliceExpr : public Expr {
public:
    static ListSliceExpr* create(CypherAST* ast, Expr* base, Expr* from, Expr* to);

    Expr* getBase() const { return _base; }
    Expr* getFrom() const { return _from; }
    Expr* getTo() const { return _to; }

private:
    Expr* _base {nullptr};
    Expr* _from {nullptr};
    Expr* _to {nullptr};

    ListSliceExpr(Expr* base, Expr* from, Expr* to);

    ~ListSliceExpr() override;
};

}
