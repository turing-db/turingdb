#include "SkipProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/Dataframe.h"

#include "PipelineV2.h"
#include "PipelinePort.h"

using namespace db::v2;

SkipProcessor::SkipProcessor(size_t skipCount)
    : _skipCount(skipCount)
{
}

SkipProcessor::~SkipProcessor() {
}

std::string SkipProcessor::describe() const {
    return fmt::format("SkipProcessor @={}", fmt::ptr(this));
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
    markAsReset();
}

void SkipProcessor::execute() {
    _input.getPort()->consume();

    if (_skipping) {
        const Dataframe* inputDf = _input.getDataframe();
        const size_t blockRowCount = inputDf->getRowCount();
        const size_t newRowCount = _currentRowCount + blockRowCount;

        if (newRowCount > _skipCount) {
            // We have crossed the skipping threshold in this block
            // write the rows that are in excess of the amount to skip
            Dataframe* outputDf = _output.getDataframe();
            const size_t rowsToWrite = newRowCount - _skipCount;
            const size_t startRow = blockRowCount - rowsToWrite;
            outputDf->copyFromLine(inputDf, startRow, rowsToWrite);

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
        const Dataframe* inputDf = _input.getDataframe();
        Dataframe* outputDf = _output.getDataframe();
        outputDf->copyFrom(inputDf);

        _output.getPort()->writeData();
    }

    finish();
}
