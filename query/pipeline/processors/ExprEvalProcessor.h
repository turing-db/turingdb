#pragma once

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class ExecutionContext;
class ExprProgram;

class ExprEvalProcessor final : public Processor {
public:
    static ExprEvalProcessor* create(PipelineV2* pipeline,
                                        ExprProgram* exprProg);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;
    ExprProgram* _exprProg {nullptr};

    ExprEvalProcessor(ExprProgram* exprProg);
    ~ExprEvalProcessor() final = default;
    void evalProgram();
};

}
