#include "EntityTypeExpr.h"

#include "CypherAST.h"

using namespace db;

EntityTypeExpr::EntityTypeExpr(Symbol* symbol, Expr* operand, SymbolChain* types)
    : Expr(Kind::ENTITY_TYPES),
    _symbol(symbol),
    _operand(operand),
    _types(types)
{
}

EntityTypeExpr::~EntityTypeExpr() {
}

EntityTypeExpr* EntityTypeExpr::create(CypherAST* ast, 
                                       Symbol* symbol,
                                       SymbolChain* types) {
    EntityTypeExpr* expr = new EntityTypeExpr(symbol, nullptr, types);
    ast->addExpr(expr);
    return expr;
}

EntityTypeExpr* EntityTypeExpr::create(CypherAST* ast, Expr* operand, SymbolChain* types) {
    EntityTypeExpr* expr = new EntityTypeExpr(nullptr, operand, types);
    ast->addExpr(expr);
    return expr;
}
