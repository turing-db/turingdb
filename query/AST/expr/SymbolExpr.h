#pragma once

#include "Expr.h"

namespace db::v2 {

class CypherAST;
class Symbol;
class VarDecl;

class SymbolExpr : public Expr {
public:
    static SymbolExpr* create(CypherAST* ast, Symbol* symbol);

    Symbol* getSymbol() const { return _symbol; }

    VarDecl* getDecl() const { return _decl; }

    void setSymbol(Symbol* symbol) { _symbol = symbol; }

    void setDecl(VarDecl* decl) { _decl = decl; }

private:
    Symbol* _symbol {nullptr};
    VarDecl* _decl {nullptr};

    explicit SymbolExpr(Symbol* symbol)
        : Expr(Kind::SYMBOL),
        _symbol(symbol)
    {
    }

    ~SymbolExpr() override;
};

}
