#pragma once

#include "ExprProgram.h"

namespace db {

class PipelineV2;

class FunctionProgram final : public ExprProgram {
public:
    static FunctionProgram* create(PipelineV2* pipeline);
private:
    FunctionProgram() = default;
    ~FunctionProgram() final = default;
};

}
