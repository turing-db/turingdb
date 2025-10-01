#include "CountProcessor.h"

#include "PipelinePort.h"

#include "PipelineException.h"

using namespace db::v2;

CountProcessor::CountProcessor()
{
}

CountProcessor::~CountProcessor() {
}

CountProcessor* CountProcessor::create(PipelineV2* pipeline) {
    CountProcessor* count = new CountProcessor();

    PipelineInputPort* input = PipelineInputPort::create(pipeline, count);
    count->_input = input;
    count->addInput(input);
    
    PipelineOutputPort* output = PipelineOutputPort::create(pipeline, count);
    count->_output = output;
    count->addOutput(output);

    count->postCreate(pipeline);

    return count;
}

void CountProcessor::prepare(ExecutionContext* ctxt) {
    ColumnConst<size_t>* countColumn = dynamic_cast<ColumnConst<size_t>*>(_output->getBuffer()->getBlock().columns().front());
    if (!countColumn) {
       throw PipelineException("CountProcessor::prepare: count column is not a ColumnConst<size_t>");
    }

    _countColumn = countColumn;
    markAsPrepared();
}

void CountProcessor::reset() {
    _countRunning = 0;
    markAsReset();
}

void CountProcessor::execute() {
    _input->consume();

    const Block& inputBlock = _input->getBuffer()->getBlock();
    const size_t blockRowCount = inputBlock.getBlockRowCount();
    _countRunning += blockRowCount;

    // Write the count value only if the input is finished
    if (_input->isClosed()) {
        _countColumn->set(_countRunning);
        _output->writeData();
        finish();
    }
}
