#include "PipelineV2.h"

#include "Processor.h"
#include "PipelineBuffer.h"

using namespace db::v2;

PipelineV2::PipelineV2()
{
}

PipelineV2::~PipelineV2() {
    for (Processor* processor : _processors) {
        delete processor;
    }
    for (PipelineBuffer* buffer : _buffers) {
        delete buffer;
    }
}

void PipelineV2::addProcessor(Processor* processor) {
    _processors.push_back(processor);
}

void PipelineV2::addBuffer(PipelineBuffer* buffer) {
    _buffers.push_back(buffer);
}
