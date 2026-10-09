#pragma once

#include <span>
#include <vector>

#include "Expr.h"
#include "decl/SchemaNameRef.h"

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

    // The chain as the kind of the tested entity reads it: the analyzer fills one of the two
    std::span<const LabelRef> labels() const { return _labels; }
    std::span<const EdgeTypeRef> edgeTypes() const { return _edgeTypes; }

    void collectLabels();
    void collectEdgeTypes();

private:
    Symbol* _symbol {nullptr};
    Expr* _operand {nullptr};
    SymbolChain* _types {nullptr};
    VarDecl* _entityDecl {nullptr};
    std::vector<LabelRef> _labels;
    std::vector<EdgeTypeRef> _edgeTypes;

    EntityTypeExpr(Symbol* symbol, Expr* operand, SymbolChain* types);
    ~EntityTypeExpr() override;
};

}
