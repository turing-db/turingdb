#include "ParameterExpr.h"

#include "CypherAST.h"

using namespace db::v2;

ParameterExpr::~ParameterExpr() {
}

ParameterExpr* ParameterExpr::create(CypherAST* ast, const Parameter& param) {
    ParameterExpr* expr = new ParameterExpr(param);
    ast->addExpr(expr);
    return expr;
}
