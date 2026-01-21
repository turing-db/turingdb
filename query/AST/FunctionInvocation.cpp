#include "FunctionInvocation.h"

#include "CypherAST.h"
#include "expr/ExprChain.h"

using namespace db;

FunctionInvocation* FunctionInvocation::create(CypherAST* ast, QualifiedName* name) {
    FunctionInvocation* invocation = new FunctionInvocation(name);
    ast->addFunctionInvocation(invocation);

    // Create empty arguments ExprChain in case the function has no arguments
    // It may be replaced later by doing setArguments, it's ok since it's all registered in CypherAST
    ExprChain* emptyArgs = ExprChain::create(ast);
    invocation->setArguments(emptyArgs);

    return invocation;
}
