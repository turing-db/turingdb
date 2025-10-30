#include "SkipProcessor.h"

#include "PipelineV2.h"
#include "PipelinePort.h"

using namespace db::v2;

SkipProcessor::SkipProcessor(size_t skipCount)
    : _skipCount(skipCount)
{
}

SkipProcessor::~SkipProcessor() {
}

SkipProcessor* SkipProcessor::create(PipelineV2* pipeline, size_t skipCount) {
    SkipProcessor* skip = new SkipProcessor(skipCount);

    PipelineInputPort* input = PipelineInputPort::create(pipeline, skip);
    skip->_input.setPort(input);
    skip->addInput(input);

    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, skip);
    skip->_output.setPort(output);
    skip->addOutput(output);

    skip->postCreate(pipeline);

    return skip;
}

void SkipProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void SkipProcessor::reset() {
    _currentRowCount = 0;
    _skipping = true;
}

void SkipProcessor::execute() {
    _input.getPort()->consume();

    if (_skipping) {
        const Block& inputBlock = _input.getPort()->getBuffer()->getBlock();
        const size_t blockRowCount = inputBlock.getBlockRowCount();
        const size_t newRowCount = _currentRowCount + blockRowCount;

        if (newRowCount > _skipCount) {
            // We have crossed the skipping threshold in this block
            // write the rows that are in excess of the amount to skip
            Block& outputBlock = _output.getPort()->getBuffer()->getBlock();
            const size_t rowsToWrite = newRowCount - _skipCount;
            const size_t startRow = blockRowCount - rowsToWrite;
            outputBlock.assignFromLine(inputBlock, startRow, rowsToWrite);

            _skipping = false;
            _currentRowCount += rowsToWrite;
            _output.getPort()->writeData();
        } else {
            // We are below the number of lines to be skipped
            // skip this block, do not write anything on the output
            _currentRowCount = newRowCount;
        }
    } else {
        // We are beyond the number of lines to be skipped, just copy the input block
        const Block& inputBlock = _input.getPort()->getBuffer()->getBlock();
        Block& outputBlock = _output.getPort()->getBuffer()->getBlock();
        outputBlock.assignFrom(inputBlock);

        _output.getPort()->writeData();
    }

    finish();
}
