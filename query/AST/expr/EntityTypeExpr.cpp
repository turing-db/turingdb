#include "EntityTypeExpr.h"

#include "CypherAST.h"

using namespace db;

EntityTypeExpr::EntityTypeExpr(Symbol* symbol, Expr* operand, LabelExpr* labelExpr)
    : Expr(Kind::ENTITY_TYPES),
    _symbol(symbol),
    _operand(operand),
    _labelExpr(labelExpr)
{
}

EntityTypeExpr::~EntityTypeExpr() {
}

EntityTypeExpr* EntityTypeExpr::create(CypherAST* ast, 
                                       Symbol* symbol,
                                       LabelExpr* labelExpr) {
    EntityTypeExpr* expr = new EntityTypeExpr(symbol, nullptr, labelExpr);
    ast->addExpr(expr);
    return expr;
}

EntityTypeExpr* EntityTypeExpr::create(CypherAST* ast, Expr* operand, LabelExpr* labelExpr) {
    EntityTypeExpr* expr = new EntityTypeExpr(nullptr, operand, labelExpr);
    ast->addExpr(expr);
    return expr;
}
