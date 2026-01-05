#include "FunctionInvocationExpr.h"

#include "CypherAST.h"
#include "FunctionInvocation.h"

using namespace db::v2;

FunctionInvocationExpr::~FunctionInvocationExpr() {
}

FunctionInvocationExpr* FunctionInvocationExpr::create(CypherAST* ast, FunctionInvocation* invocation) {
    FunctionInvocationExpr* expr = new FunctionInvocationExpr(invocation);
    ast->addExpr(expr);
    return expr;
}

void FunctionInvocationExpr::setSignature(FunctionSignature* signature) {
    _invocation->setSignature(signature);
}
