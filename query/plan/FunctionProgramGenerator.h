#pragma once

#include "ExprProgramGenerator.h"
#include "expr/FunctionInvocationExpr.h"

namespace db {

class FunctionProgramGenerator final : public ExprProgramGenerator {
public:
    FunctionProgramGenerator(PipelineGenerator* gen,
                              ExprProgram* exprProg,
                              const PendingOutputView& pendingOut)
        : ExprProgramGenerator(gen, exprProg, pendingOut)
    {
    }

    Column* generateFuncInvocationExpr(const FunctionInvocationExpr* funcExpr) {
        return ExprProgramGenerator::generateFuncInvocationExpr(funcExpr);
    }

    ~FunctionProgramGenerator() final = default;
};

}
