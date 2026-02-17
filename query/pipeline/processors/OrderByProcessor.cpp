#include "OrderByProcessor.h"

#include <spdlog/fmt/bundled/format.h>

#include "PipelinePort.h"

using namespace db;

OrderByProcessor::OrderByProcessor()
{
}

OrderByProcessor::~OrderByProcessor() {
}

std::string OrderByProcessor::describe() const {
    return fmt::format("OrderByProcessor@={}", fmt::ptr(this));
}

OrderByProcessor* OrderByProcessor::create(PipelineV2* pipeline,
                                           std::span<OrderByKey> keys) {
    OrderByProcessor* proc = new OrderByProcessor;

    {
        PipelineInputPort* inputPort = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(inputPort);
        proc->addInput(inputPort);
    }

    {
        PipelineOutputPort* outputPort = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(outputPort);
        proc->addOutput(outputPort);
    }

    {
        proc->_orderedKeys.reserve(keys.size());
        proc->_orderedKeys.assign(begin(keys), end(keys));
    }
    proc->postCreate(pipeline);
    return proc;
}

void OrderByProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void OrderByProcessor::reset() {
    markAsReset();
}

// TODO:
// - Handle ColumnConst as order key
void OrderByProcessor::execute() {
}

