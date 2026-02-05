#include "ExprEvalProcessor.h"

#include <iostream>
#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "ExprProgram.h"
#include "dataframe/Dataframe.h"
#include "interfaces/PipelineBlockInputInterface.h"

using namespace db;

ExprEvalProcessor::ExprEvalProcessor(ExprProgram* exprProg)
    : _exprProg(exprProg)
{
}

std::string ExprEvalProcessor::describe() const {
    return fmt::format("ComputeExprProcessor @={}", fmt::ptr(this));
}

ExprEvalProcessor* ExprEvalProcessor::create(PipelineV2* pipeline,
                                             ExprProgram* exprProg,
                                             bool hasInput) {
    ExprEvalProcessor* proc = new ExprEvalProcessor(exprProg);

    if (hasInput) {
        proc->_input = std::make_optional<PipelineBlockInputInterface>();

        PipelineInputPort* inputPort = PipelineInputPort::create(pipeline, proc);

        proc->_input->setPort(inputPort);
        proc->addInput(inputPort);
        inputPort->setNeedsData(true);
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
    if (_input) {
        _input->getPort()->consume();
    }
    _output.getPort()->writeData();

    _exprProg->evaluateInstructions();

    finish();
}
