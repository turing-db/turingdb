#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class Symbol;
class SymbolChain;
class VarDecl;

class EntityTypeExpr : public Expr {
public:
    Symbol* getSymbol() const { return _symbol; }
    Expr* getOperand() const { return _operand; }
    SymbolChain* getTypes() const { return _types; }

    static EntityTypeExpr* create(CypherAST* ast, 
                                 Symbol* symbol,
                                 SymbolChain* types);

    static EntityTypeExpr* create(CypherAST* ast, Expr* operand, SymbolChain* types);

    void setEntityDecl(VarDecl* decl) { _entityDecl = decl; }

    VarDecl* getEntityVarDecl() const { return _entityDecl; }

private:
    Symbol* _symbol {nullptr};
    Expr* _operand {nullptr};
    SymbolChain* _types {nullptr};
    VarDecl* _entityDecl {nullptr};

    EntityTypeExpr(Symbol* symbol, Expr* operand, SymbolChain* types);
    ~EntityTypeExpr() override;
};

}
