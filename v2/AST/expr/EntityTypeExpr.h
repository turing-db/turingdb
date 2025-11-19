#pragma once

#include "Expr.h"

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

    void setEntityDecl(VarDecl* decl) { _entityDecl = decl; }

    VarDecl* getEntityVarDecl() const { return _entityDecl; }

private:
    Symbol* _symbol {nullptr};
    SymbolChain* _types;
    VarDecl* _entityDecl {nullptr};

    EntityTypeExpr(Symbol* symbol, SymbolChain* types);
    ~EntityTypeExpr() override;
};

}
