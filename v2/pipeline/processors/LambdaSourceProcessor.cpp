#include "LambdaSourceProcessor.h"

using namespace db::v2;

LambdaSourceProcessor::LambdaSourceProcessor(Callback callback)
    : _callback(callback)
{
}

LambdaSourceProcessor::~LambdaSourceProcessor() {
}

void LambdaSourceProcessor::prepare(ExecutionContext* ctxt) {
    bool isFinished = false;
    _callback(_output->getBuffer()->getBlock(), isFinished, Operation::PREPARE);
    markAsPrepared();
}

void LambdaSourceProcessor::reset() {
    bool isFinished = false;
    _callback(_output->getBuffer()->getBlock(), isFinished, Operation::RESET);
    markAsReset();
}

void LambdaSourceProcessor::execute() {
    bool isFinished = false;
    _callback(_output->getBuffer()->getBlock(), isFinished, Operation::EXECUTE);

    if (isFinished) {
        finish();
    }
}

LambdaSourceProcessor* LambdaSourceProcessor::create(PipelineV2* pipeline, Callback callback) {
    LambdaSourceProcessor* lambda = new LambdaSourceProcessor(callback);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, lambda);
    lambda->_output = output;
    lambda->addOutput(output);

    lambda->postCreate(pipeline);
    return lambda;
}
