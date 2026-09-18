#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Symbol;
class VarDecl;

// A Cypher list comprehension: `[x IN xs WHERE p(x) | f(x)]`. The variable is bound to
// each element of the source list in turn, the predicate drops the elements it does not
// hold for, and the projection is the value each surviving element contributes. Both the
// predicate and the projection are optional: without a predicate every element survives,
// without a projection each element contributes itself.
class ListComprehensionExpr : public Expr {
public:
    const Symbol* getSymbol() const { return _symbol; }
    Expr* getSource() const { return _source; }
    Expr* getPredicate() const { return _predicate; }
    Expr* getProjection() const { return _projection; }
    const VarDecl* getDecl() const { return _decl; }

    void setPredicate(Expr* predicate) { _predicate = predicate; }
    void setProjection(Expr* projection) { _projection = projection; }
    void setDecl(const VarDecl* decl) { _decl = decl; }

    static ListComprehensionExpr* create(CypherAST* ast, Symbol* symbol, Expr* source);

private:
    Symbol* _symbol {nullptr};
    Expr* _source {nullptr};
    Expr* _predicate {nullptr};
    Expr* _projection {nullptr};
    const VarDecl* _decl {nullptr};

    ListComprehensionExpr(Symbol* symbol, Expr* source);
    ~ListComprehensionExpr() override;
};

}
