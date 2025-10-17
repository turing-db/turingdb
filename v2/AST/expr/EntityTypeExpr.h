#pragma once

#include "Expr.h"

#include <vector>

namespace db::v2 {

class CypherAST;
class Symbol;
class SymbolChain;
class VarDecl;

class EntityTypeExpr : public Expr {
public:
    Symbol* getSymbol() const { return _symbol; }
    SymbolChain* getTypes() const { return _types; }

    static EntityTypeExpr* create(CypherAST* ast, 
                                 Symbol* symbol,
                                 SymbolChain* types);

    VarDecl* getDecl() const { return _decl; }

    void setDecl(VarDecl* decl) { _decl = decl; }

private:
    Symbol* _symbol {nullptr};
    SymbolChain* _types;
    VarDecl* _decl {nullptr};

    EntityTypeExpr(Symbol* symbol, SymbolChain* types);
    ~EntityTypeExpr() override;
};

}
