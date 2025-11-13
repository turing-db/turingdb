#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

namespace db::v2 {

class ExecutionContext;
class EvalProgram;

class EvalExprProcessor : public Processor {
public:
    static EvalExprProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;
    std::unique_ptr<EvalProgram> _prog;

    EvalExprProcessor();
    ~EvalExprProcessor();
    void evalProgram();
};

}
