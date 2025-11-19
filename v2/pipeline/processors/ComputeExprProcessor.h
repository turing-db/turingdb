#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValuesOutputInterface.h"

namespace db::v2 {

class ExecutionContext;
class ExprProgram;

class ComputeExprProcessor : public Processor {
public:
    static ComputeExprProcessor* create(PipelineV2* pipeline,
                                        ExprProgram* exprProg);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _input;
    PipelineValuesOutputInterface _output;
    ExprProgram* _exprProg {nullptr};

    ComputeExprProcessor(ExprProgram* exprProg);
    ~ComputeExprProcessor();
    void evalProgram();
};

}
