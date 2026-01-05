#include "LiteralExpr.h"

#include "CypherAST.h"
#include "Literal.h"

using namespace db::v2;

LiteralExpr::~LiteralExpr() {
}

LiteralExpr* LiteralExpr::create(CypherAST* ast, Literal* literal) {
    LiteralExpr* expr = new LiteralExpr(literal);
    ast->addExpr(expr);

    return expr;
}

EvaluatedType LiteralExpr::getType() const {
    return _literal->getType();
}
