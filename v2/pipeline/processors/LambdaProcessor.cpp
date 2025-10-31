#include "LambdaProcessor.h"

using namespace db::v2;

LambdaProcessor::LambdaProcessor(Callback callback)
    : _callback(callback)
{
}

LambdaProcessor::~LambdaProcessor() {
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

LambdaProcessor* LambdaProcessor::create(PipelineV2* pipeline, Callback callback) {
    LambdaProcessor* lambda = new LambdaProcessor(callback);

    PipelineInputPort* input = PipelineInputPort::create(pipeline, lambda);
    lambda->_input.setPort(input);
    lambda->addInput(input);

    lambda->postCreate(pipeline);
    return lambda;
}
