#pragma once

#include "Expr.h"

#include <vector>

namespace db::v2 {

class CypherAST;
class Symbol;
class VarDecl;

class EntityTypeExpr : public Expr {
public:
    using Types = std::vector<Symbol*>;

    Symbol* getSymbol() const { return _symbol; }
    const Types& getTypes() const { return _types; }

    static EntityTypeExpr* create(CypherAST* ast, 
                                 Symbol* symbol,
                                 Types&& types);

    VarDecl* getDecl() const { return _decl; }

    void setDecl(VarDecl* decl) { _decl = decl; }

private:
    Symbol* _symbol {nullptr};
    Types _types;
    VarDecl* _decl {nullptr};

    EntityTypeExpr(Symbol* symbol, Types&& types);
    ~EntityTypeExpr() override;
};

}
