#pragma once

#include <optional>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class ExecutionContext;
class ExprProgram;

/**
 * @brief Standalone processor taking an @ref ExprProgram to execute.
 * @detail Used in RETURN projections. NOTE: Not used in @ref FilterProcessor, which
 * instead evaluates expressions itself.
 */
class ExprEvalProcessor final : public Processor {
public:
    static ExprEvalProcessor* create(PipelineV2* pipeline,
                                     ExprProgram* exprProg,
                                     bool hasInput = false);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineBlockInputInterface& input();

    PipelineBlockOutputInterface& output() { return _output; }

private:
    /// May not have input in case of standalone return, e.g. `RETURN 4 + 5`
    std::optional<PipelineBlockInputInterface> _input;
    PipelineBlockOutputInterface _output;
    ExprProgram* _exprProg {nullptr};

    ExprEvalProcessor(ExprProgram* exprProg);
    ~ExprEvalProcessor() final;
};

}
