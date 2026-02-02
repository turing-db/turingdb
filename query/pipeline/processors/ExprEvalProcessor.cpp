#include "ExprEvalProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "ExprProgram.h"

using namespace db;

ExprEvalProcessor::ExprEvalProcessor(ExprProgram* exprProg)
    : _exprProg(exprProg)
{
}

std::string ExprEvalProcessor::describe() const {
    return fmt::format("ComputeExprProcessor @={}", fmt::ptr(this));
}

ExprEvalProcessor* ExprEvalProcessor::create(PipelineV2* pipeline,
                                                   ExprProgram* exprProg) {
    ExprEvalProcessor* proc = new ExprEvalProcessor(exprProg);

    {
        PipelineInputPort* input = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(input);
        proc->addInput(input);
    }

    {
        PipelineOutputPort* output = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(output);
        proc->addOutput(output);
    }

    proc->postCreate(pipeline);

    return proc;
}

void ExprEvalProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void ExprEvalProcessor::reset() {
    markAsReset();
}

void ExprEvalProcessor::execute() {
    _input.getPort()->consume();
    _output.getPort()->writeData();

    _exprProg->evaluateInstructions();

    finish();
}
