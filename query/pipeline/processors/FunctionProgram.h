#pragma once

#include "ExprProgram.h"

#include "views/GraphView.h"

namespace db {

class PipelineV2;

class FunctionProgram final : public ExprProgram {
public:
    static FunctionProgram* create(PipelineV2* pipeline);

    void setView(GraphView view) { _view = view; }

    void evaluateInstructions() final;
private:
    GraphView _view;

    void evalInstr(const Instruction& instr) final;
    void evalFunction(const Instruction& instr);

    FunctionProgram() = default;
    ~FunctionProgram() final = default;
};

}
