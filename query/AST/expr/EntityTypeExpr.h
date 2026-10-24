#pragma once

#include "Expr.h"

namespace db {

class CypherAST;
class LabelExpr;
class Symbol;
class VarDecl;

class EntityTypeExpr : public Expr {
public:
    Symbol* getSymbol() const { return _symbol; }
    Expr* getOperand() const { return _operand; }
    LabelExpr* getLabelExpr() const { return _labelExpr; }

    static EntityTypeExpr* create(CypherAST* ast, 
                                 Symbol* symbol,
                                 LabelExpr* labelExpr);

    static EntityTypeExpr* create(CypherAST* ast, Expr* operand, LabelExpr* labelExpr);

    void setEntityDecl(VarDecl* decl) { _entityDecl = decl; }

    VarDecl* getEntityVarDecl() const { return _entityDecl; }

private:
    Symbol* _symbol {nullptr};
    Expr* _operand {nullptr};
    LabelExpr* _labelExpr {nullptr};
    VarDecl* _entityDecl {nullptr};

    EntityTypeExpr(Symbol* symbol, Expr* operand, LabelExpr* labelExpr);
    ~EntityTypeExpr() override;
};

}
