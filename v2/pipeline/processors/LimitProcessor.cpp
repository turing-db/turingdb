#include "LimitProcessor.h"

#include "dataframe/Dataframe.h"

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
    processor->_input.setPort(input);
    processor->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, processor);
    processor->_output.setPort(output);
    processor->addOutput(output);

    processor->postCreate(pipeline);

    return processor;
}

void LimitProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void LimitProcessor::reset() {
    _currentRowCount = 0;
    _reachedLimit = false;
    markAsReset();
}

void LimitProcessor::execute() {
    _input.getPort()->consume();
    finish();

    if (_reachedLimit) {
        // We have reached the limit, do nothing
        return;
    } else {
        const Dataframe* inputDf = _input.getDataframe();
        Dataframe* outputDf = _output.getDataframe();
        const size_t blockRowCount = inputDf->getRowCount();
        const size_t remainingCapacity = _limit - _currentRowCount;

        // Write rows of inputBlock that are below the limit
        const size_t rowsToWrite = std::min(blockRowCount, remainingCapacity);
        outputDf->copyFromLine(inputDf, 0, rowsToWrite);

        if (blockRowCount >= remainingCapacity) {
            _reachedLimit = true;
        }

        _currentRowCount += rowsToWrite;
        _output.getPort()->writeData();
    }
}
