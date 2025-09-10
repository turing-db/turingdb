#include "LiteralExpr.h"

#include "CypherAST.h"

using namespace db::v2;

LiteralExpr::~LiteralExpr() {
}

LiteralExpr* LiteralExpr::create(CypherAST* ast, const Literal& literal) {
    LiteralExpr* expr = new LiteralExpr(literal);
    ast->addExpr(expr);

    return expr;
}
