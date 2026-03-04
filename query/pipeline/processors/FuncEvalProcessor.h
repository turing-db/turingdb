#pragma once

#include <optional>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class ExecutionContext;
class FunctionProgram;

class FuncEvalProcessor final : public Processor {
public:
    static FuncEvalProcessor* create(PipelineV2* pipeline,
                                     FunctionProgram* funcProg,
                                     bool hasInput = false);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockInputInterface& input();

    PipelineBlockOutputInterface& output() { return _output; }

private:
    /// May not have input in case of standalone return, e.g. `RETURN sqrt(9)`
    std::optional<PipelineBlockInputInterface> _input;
    PipelineBlockOutputInterface _output;

    FunctionProgram* _funcProg {nullptr};

    FuncEvalProcessor(FunctionProgram* funcProg);
    ~FuncEvalProcessor() final;
};

}
