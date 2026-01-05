#include "LambdaProcessor.h"

#include <spdlog/fmt/fmt.h>

using namespace db::v2;

LambdaProcessor::LambdaProcessor(const Callback& callback)
    : _callback(callback)
{
}

LambdaProcessor::~LambdaProcessor() {
}

std::string LambdaProcessor::describe() const {
    return fmt::format("LambdaProcessor @={}", fmt::ptr(this));
}

void LambdaProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void LambdaProcessor::reset() {
    _callback(_input.getDataframe(), Operation::RESET);
    markAsReset();
}

void LambdaProcessor::execute() {
    _input.getPort()->consume();
    _callback(_input.getDataframe(), Operation::EXECUTE);
    finish();
}

LambdaProcessor* LambdaProcessor::create(PipelineV2* pipeline, const Callback& callback) {
    LambdaProcessor* lambda = new LambdaProcessor(callback);

    PipelineInputPort* input = PipelineInputPort::create(pipeline, lambda);
    lambda->_input.setPort(input);
    lambda->addInput(input);

    lambda->postCreate(pipeline);
    return lambda;
}
