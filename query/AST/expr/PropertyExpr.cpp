#include "PropertyExpr.h"

#include "CypherAST.h"

using namespace db;

PropertyExpr::~PropertyExpr() {
}

PropertyExpr* PropertyExpr::create(CypherAST* ast, QualifiedName* name) {
    PropertyExpr* expr = new PropertyExpr(name);
    ast->addExpr(expr);
    return expr;
}

void PropertyExpr::setMapKey(std::string_view key) {
    _mapKey = key;
    _readsAMapKey = true;
}
