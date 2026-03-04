#pragma once

#include "ExprProgramGenerator.h"

namespace db {

class FunctionProgramGenerator final : public ExprProgramGenerator {
public:
    FunctionProgramGenerator(PipelineGenerator* gen,
                              ExprProgram* exprProg,
                              const PendingOutputView& pendingOut)
        : ExprProgramGenerator(gen, exprProg, pendingOut)
    {
    }

    ~FunctionProgramGenerator() final = default;
};

}
