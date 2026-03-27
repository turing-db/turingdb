#include "IndexLookupProcessor.h"

#include <spdlog/fmt/bundled/format.h>

#include "PipelinePort.h"

using namespace db;

IndexLookupProcessor::IndexLookupProcessor(const Index* index)
    : _index(index)
{
}

std::string IndexLookupProcessor::describe() const  {
    return fmt::format("IndexLookupProcessor @={}, Index @={}", fmt::ptr(this), fmt::ptr(_index));
}

IndexLookupProcessor* IndexLookupProcessor::create(PipelineV2* pipeline,
                                                   const Index* index) {
    IndexLookupProcessor* proc = new IndexLookupProcessor(index);

    {
        PipelineInputPort* in = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(in);
        proc->addInput(in);
    }

    {
        PipelineOutputPort* out = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(out);
        proc->addOutput(out);
    }

    proc->postCreate(pipeline);

    return proc;
}

void IndexLookupProcessor::prepare(ExecutionContext* ctxt){
    // TODO: Check validity of idex
    markAsPrepared();
}

void IndexLookupProcessor::reset() {
    markAsReset();
}

void IndexLookupProcessor::execute() {
}

