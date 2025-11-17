#include "LambdaSourceProcessor.h"

#include <spdlog/spdlog.h>

using namespace db::v2;

LambdaSourceProcessor::LambdaSourceProcessor(Callback callback)
    : _callback(callback),
    _output(PipelineBlockOutputInterface::createOutputOnly())
{
}

LambdaSourceProcessor::~LambdaSourceProcessor() {
}

std::string LambdaSourceProcessor::describe() const {
    return fmt::format("LambdaSourceProcessor @={}", fmt::ptr(this));
}

void LambdaSourceProcessor::prepare(ExecutionContext* ctxt) {
    bool isFinished = false;
    _callback(_output.getDataframe(), isFinished, Operation::PREPARE);
    markAsPrepared();
}

void LambdaSourceProcessor::reset() {
    bool isFinished = false;
    _callback(_output.getDataframe(), isFinished, Operation::RESET);
    markAsReset();
}

void LambdaSourceProcessor::execute() {
    bool isFinished = false;
    _callback(_output.getDataframe(), isFinished, Operation::EXECUTE);
    _output.getPort()->writeData();

    if (isFinished) {
        finish();
    }
}

LambdaSourceProcessor* LambdaSourceProcessor::create(PipelineV2* pipeline, Callback callback) {
    LambdaSourceProcessor* lambda = new LambdaSourceProcessor(callback);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, lambda);
    lambda->_output.setPort(output);
    lambda->addOutput(output);

    lambda->postCreate(pipeline);
    return lambda;
}
