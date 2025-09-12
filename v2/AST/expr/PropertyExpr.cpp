#include "PropertyExpr.h"

#include "CypherAST.h"

using namespace db::v2;

PropertyExpr::~PropertyExpr() {
}

PropertyExpr* PropertyExpr::create(CypherAST* ast, QualifiedName* name) {
    PropertyExpr* expr = new PropertyExpr(name);
    ast->addExpr(expr);
    return expr;
}
