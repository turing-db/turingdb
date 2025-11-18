#include "CountProcessor.h"

#include <spdlog/fmt/fmt.h>

#include "dataframe/Dataframe.h"
#include "columns/ColumnConst.h"
#include "dataframe/NamedColumn.h"

#include "PipelineException.h"

using namespace db::v2;

CountProcessor::CountProcessor()
{
}

CountProcessor::~CountProcessor() {
}

std::string CountProcessor::describe() const {
    return fmt::format("CountProcessor @={}", fmt::ptr(this));
}

CountProcessor* CountProcessor::create(PipelineV2* pipeline) {
    CountProcessor* count = new CountProcessor();

    PipelineInputPort* input = PipelineInputPort::create(pipeline, count);
    count->_input.setPort(input);
    count->addInput(input);
    
    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, count);
    count->_output.setPort(output);
    count->addOutput(output);

    count->postCreate(pipeline);

    return count;
}

void CountProcessor::prepare(ExecutionContext* ctxt) {
    ColumnConst<size_t>* countColumn = dynamic_cast<ColumnConst<size_t>*>(_output.getValue()->getColumn());
    if (!countColumn) {
        throw PipelineException("CountProcessor: count column is not a ColumnConst<size_t>");
    }

    _countColumn = countColumn;
    markAsPrepared();
}

void CountProcessor::reset() {
    markAsReset();
}

void CountProcessor::execute() {
    PipelineInputPort* inputPort = _input.getPort();
    inputPort->consume();

    const Dataframe* inputDf = _input.getDataframe();
    const size_t blockRowCount = inputDf->getRowCount();
    _countRunning += blockRowCount;

    // Write the count value only if the input is finished
    if (inputPort->isClosed()) {
        _countColumn->set(_countRunning);
        _output.getPort()->writeData();
        finish();
    }
}
