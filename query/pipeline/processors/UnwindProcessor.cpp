#include "UnwindProcessor.h"

#include "BioAssert.h"
#include "PipelinePort.h"

using namespace db;

UnwindProcessor::UnwindProcessor(ListView list)
    : _list(list)
{
}

UnwindProcessor::~UnwindProcessor() {
}

UnwindProcessor* UnwindProcessor::create(PipelineV2* pipeline, ListView list) {
    auto* proc = new UnwindProcessor(list);

    {
        PipelineOutputPort* outPort = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(outPort);
        proc->addOutput(outPort);
    }

    proc->postCreate(pipeline);

    return proc;
}

void UnwindProcessor::prepare(ExecutionContext*) {
    markAsPrepared();
}

void UnwindProcessor::reset() {
    markAsReset();
}

void UnwindProcessor::execute() {
    bioassert(_list, "Invalid list in UNWIND.");
}
