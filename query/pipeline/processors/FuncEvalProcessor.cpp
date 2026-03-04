#include "FuncEvalProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "PipelineV2.h"
#include "FunctionProgram.h"
#include "interfaces/PipelineBlockInputInterface.h"

#include "FatalException.h"

using namespace db;

FuncEvalProcessor::FuncEvalProcessor(FunctionProgram* funcProg)
    : _funcProg(funcProg)
{
}

FuncEvalProcessor::~FuncEvalProcessor() {
}

FuncEvalProcessor* FuncEvalProcessor::create(PipelineV2* pipeline,
                                     FunctionProgram* funcProg,
                                     bool hasInput) {
    FuncEvalProcessor* proc = new FuncEvalProcessor(funcProg);

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

PipelineBlockInputInterface& FuncEvalProcessor::input() {
    if (!_input) {
        throw FatalException("Attempted to get null input of FuncEvalProcessor.");
    }
    return *_input;
}

std::string FuncEvalProcessor::describe() const {
    return fmt::format("FuncEvalProcessor@={}", fmt::ptr(this));
}


void FuncEvalProcessor::prepare(ExecutionContext* /*ctxt*/) {
    markAsPrepared();
}

void FuncEvalProcessor::reset() {
    markAsReset();
}

void FuncEvalProcessor::execute() {
    _funcProg->evaluateInstructions();

    if (_input) {
        _input->getPort()->consume();
    }

    _output.getPort()->writeData();

    finish();
}
