#include "LambdaProcessor.h"

using namespace db::v2;

LambdaProcessor::LambdaProcessor(Callback callback)
    : _callback(callback)
{
}

LambdaProcessor::~LambdaProcessor() {
}

void LambdaProcessor::prepare(ExecutionContext* ctxt) {
}

void LambdaProcessor::reset() {
    _callback(_input->getBuffer()->getBlock(), Operation::RESET);
}

void LambdaProcessor::execute() {
    _input->consume();
    _callback(_input->getBuffer()->getBlock(), Operation::EXECUTE);
}

LambdaProcessor* LambdaProcessor::create(PipelineV2* pipeline, Callback callback) {
    LambdaProcessor* lambda = new LambdaProcessor(callback);

    PipelinePort* input = PipelinePort::create(pipeline, lambda);
    lambda->_input = input;
    lambda->addInput(input);

    lambda->postCreate(pipeline);
    return lambda;
}
