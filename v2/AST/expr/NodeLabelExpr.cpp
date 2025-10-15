#include "NodeLabelExpr.h"

#include "CypherAST.h"

using namespace db::v2;

EntityTypeExpr::EntityTypeExpr(Symbol* symbol, Types&& types)
    : Expr(Kind::ENTITY_TYPES),
    _symbol(symbol),
    _types(std::move(types))
{
}

EntityTypeExpr::~EntityTypeExpr() {
}

EntityTypeExpr* EntityTypeExpr::create(CypherAST* ast, 
                                     Symbol* symbol,
                                     Types&& types) {
    EntityTypeExpr* expr = new EntityTypeExpr(symbol, std::move(types));
    ast->addExpr(expr);
    return expr;
}
