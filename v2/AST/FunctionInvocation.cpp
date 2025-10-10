#include "FunctionInvocation.h"

#include "CypherAST.h"

using namespace db::v2;

FunctionInvocation* FunctionInvocation::create(CypherAST* ast, QualifiedName* name) {
    FunctionInvocation* invocation = new FunctionInvocation(name);
    ast->addFunctionInvocation(invocation);
    return invocation;
}
