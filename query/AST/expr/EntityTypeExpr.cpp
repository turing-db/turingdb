#include "EntityTypeExpr.h"

#include "CypherAST.h"

using namespace db::v2;

EntityTypeExpr::EntityTypeExpr(Symbol* symbol, SymbolChain* types)
    : Expr(Kind::ENTITY_TYPES),
    _symbol(symbol),
    _types(types)
{
}

EntityTypeExpr::~EntityTypeExpr() {
}

EntityTypeExpr* EntityTypeExpr::create(CypherAST* ast, 
                                       Symbol* symbol,
                                       SymbolChain* types) {
    EntityTypeExpr* expr = new EntityTypeExpr(symbol, types);
    ast->addExpr(expr);
    return expr;
}
