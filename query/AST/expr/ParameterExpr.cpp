#include "ParameterExpr.h"

#include "CypherAST.h"

using namespace db;

ParameterExpr::~ParameterExpr() {
}

ParameterExpr* ParameterExpr::create(CypherAST* ast, Parameter* parameter) {
    ParameterExpr* expr = new ParameterExpr(parameter);
    ast->addExpr(expr);

    return expr;
}
