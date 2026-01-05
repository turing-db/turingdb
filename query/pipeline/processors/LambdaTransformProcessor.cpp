#include "LambdaTransformProcessor.h"

#include <spdlog/fmt/fmt.h>

using namespace db::v2;

LambdaTransformProcessor::LambdaTransformProcessor(Callback callback)
    : _callback(callback)
{
}

LambdaTransformProcessor::~LambdaTransformProcessor() {
}

std::string LambdaTransformProcessor::describe() const {
    return fmt::format("LambdaTransformProcessor @={}", fmt::ptr(this));
}

void LambdaTransformProcessor::prepare(ExecutionContext* ctxt) {
    bool isFinished = false;
    _callback(_input.getDataframe(), _output.getDataframe(), isFinished, Operation::PREPARE);
    markAsPrepared();
}

void LambdaTransformProcessor::reset() {
    bool isFinished = false;
    _callback(_input.getDataframe(), _output.getDataframe(), isFinished, Operation::RESET);
    markAsReset();
}

void LambdaTransformProcessor::execute() {
    bool isFinished = false;
    _callback(_input.getDataframe(), _output.getDataframe(), isFinished, Operation::EXECUTE);
    _output.getPort()->writeData();

    if (isFinished) {
        _input.getPort()->consume();
        finish();
    }
}

LambdaTransformProcessor* LambdaTransformProcessor::create(PipelineV2* pipeline, Callback callback) {
    LambdaTransformProcessor* lambda = new LambdaTransformProcessor(callback);

    PipelineInputPort* input = PipelineInputPort::create(pipeline, lambda);
    lambda->_input.setPort(input);
    lambda->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, lambda);
    lambda->_output.setPort(output);
    lambda->addOutput(output);

    lambda->postCreate(pipeline);
    return lambda;
}
