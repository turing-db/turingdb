#include "ProcedureProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineException.h"

using namespace db::v2;

ProcedureProcessor::ProcedureProcessor(const Callback& callback)
    : _callback(callback)
{
}

ProcedureProcessor::~ProcedureProcessor() {
}

std::string ProcedureProcessor::describe() const {
    return fmt::format("ProcedureProcessor @={}", fmt::ptr(this));
}

void ProcedureProcessor::prepare(ExecutionContext* ctxt) {
    _procedure.setContext(ctxt);
    _procedure.setStep(Procedure::Step::PREPARE);
    _procedure.setOutput(_output);
    _callback(_procedure);

    if (_procedure.isFinished()) [[unlikely]] {
        throw PipelineException("Cannot finish a procedure in the prepare phase");
    }

    markAsPrepared();
}

void ProcedureProcessor::reset() {
    _procedure.setStep(Procedure::Step::RESET);
    _callback(_procedure);

    if (_procedure.isFinished()) [[unlikely]] {
        throw PipelineException("Cannot finish a procedure in the reset phase");
    }

    markAsReset();
}

void ProcedureProcessor::execute() {
    _procedure.setStep(Procedure::Step::EXECUTE);
    _callback(_procedure);
    _output.getPort()->writeData();

    if (_procedure.isFinished()) {
        finish();
    }
}

ProcedureProcessor* ProcedureProcessor::create(PipelineV2* pipeline, const Callback& callback) {
    ProcedureProcessor* lambda = new ProcedureProcessor(callback);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, lambda);
    lambda->_output.setPort(output);
    lambda->addOutput(output);

    lambda->postCreate(pipeline);
    return lambda;
}
