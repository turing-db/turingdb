#pragma once

#include "Expr.h"

namespace db::v2 {

class CypherAST;
class Symbol;

class SymbolExpr : public Expr {
public:
    static SymbolExpr* create(CypherAST* ast, Symbol* symbol);

    Symbol* getSymbol() const { return _symbol; }

    void setSymbol(Symbol* symbol) { _symbol = symbol; }

private:
    Symbol* _symbol {nullptr};

    explicit SymbolExpr(Symbol* symbol)
        : Expr(Kind::SYMBOL),
        _symbol(symbol)
    {
    }

    ~SymbolExpr() override;
};

}
