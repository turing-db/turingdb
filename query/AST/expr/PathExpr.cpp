#include "PathExpr.h"

#include "CypherAST.h"
#include "PatternElement.h"

using namespace db;

PathExpr::~PathExpr() {
}

void PathExpr::addEntity(EntityPattern* entity) {
    _pattern->addEntity(entity);
}

PathExpr* PathExpr::create(CypherAST* ast, PatternElement* pattern) {
    PathExpr* expr = new PathExpr(pattern);
    ast->addExpr(expr);
    return expr;
}
