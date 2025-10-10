#pragma once

#include "Expr.h"

namespace db::v2 {

class CypherAST;
class FunctionInvocation;
class VarDecl;

class FunctionInvocationExpr : public Expr {
public:
    static FunctionInvocationExpr* create(CypherAST* ast, FunctionInvocation* invocation);

    FunctionInvocation* getFunctionInvocation() const { return _invocation; }

private:
    FunctionInvocation* _invocation {nullptr};

    explicit FunctionInvocationExpr(FunctionInvocation* symbol)
        : Expr(Kind::FUNCTION_INVOCATION),
        _invocation(symbol)
    {
    }

    ~FunctionInvocationExpr() override;
};

}
