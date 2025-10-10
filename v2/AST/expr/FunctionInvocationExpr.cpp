#include "FunctionInvocationExpr.h"

#include "CypherAST.h"

using namespace db::v2;

FunctionInvocationExpr::~FunctionInvocationExpr() {
}

FunctionInvocationExpr* FunctionInvocationExpr::create(CypherAST* ast, FunctionInvocation* invocation) {
    FunctionInvocationExpr* expr = new FunctionInvocationExpr(invocation);
    ast->addExpr(expr);
    return expr;
}
