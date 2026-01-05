#include "FunctionInvocation.h"

#include "CypherAST.h"

using namespace db;

FunctionInvocation* FunctionInvocation::create(CypherAST* ast, QualifiedName* name) {
    FunctionInvocation* invocation = new FunctionInvocation(name);
    ast->addFunctionInvocation(invocation);
    return invocation;
}
