#pragma once

#include "Expr.h"

#include "Symbol.h"

namespace db::v2 {

class VarDecl;
class CypherAST;

class SymbolExpr : public Expr {
public:
    static SymbolExpr* create(CypherAST* ast, const Symbol& symbol);

    bool hasVar() const { return _var != nullptr; }

    const Symbol& symbol() const { return _symbol; }

    const VarDecl& var() const { return *_var; }

    VarDecl& var() { return *_var; }

    void setDecl(VarDecl& var) { _var = &var; }

private:
    Symbol _symbol;
    VarDecl* _var {nullptr};

    explicit SymbolExpr(const Symbol& symbol)
        : Expr(Kind::SYMBOL),
        _symbol(symbol)
    {
    }

    ~SymbolExpr() override;
};

}
