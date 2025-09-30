#include "LimitProcessor.h"

using namespace db::v2;

LimitProcessor::LimitProcessor(size_t limit)
    : _limit(limit)
{
}

LimitProcessor::~LimitProcessor() {
}

LimitProcessor* LimitProcessor::create(PipelineV2* pipeline, size_t limit) {
    LimitProcessor* processor = new LimitProcessor(limit);

    PipelineInputPort* input = PipelineInputPort::create(pipeline, processor);
    processor->_input = input;
    processor->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
    processor->_output = output;
    processor->addOutput(output);

    processor->postCreate(pipeline);

    return processor;
}

void LimitProcessor::prepare(ExecutionContext* ctxt) {
    _prepared = true;
}

void LimitProcessor::reset() {
    _currentRowCount = 0;
    _reachedLimit = false;
}

void LimitProcessor::execute() {
    _input->consume();
    _finished = true;

    if (_reachedLimit) {
        // We have reached the limit, do nothing
        return;
    } else {
        const Block& inputBlock = _input->getBuffer()->getBlock();
        Block& outputBlock = _output->getBuffer()->getBlock();
        const size_t blockRowCount = inputBlock.columns().front()->size();
        const size_t remainingCapacity = _limit - _currentRowCount;

        // Write rows of inputBlock that are below the limit
        const size_t rowsToWrite = std::min(blockRowCount, remainingCapacity);
        outputBlock.assignFromLine(inputBlock, 0, rowsToWrite);

        if (blockRowCount >= remainingCapacity) {
            _reachedLimit = true;
        }

        _currentRowCount += rowsToWrite;
        _output->writeData();
    }
}
