#include "PipelineBuffer.h"

#include "PipelineV2.h"

using namespace db::v2;

PipelineBuffer* PipelineBuffer::create(PipelineV2* pipeline) {
    PipelineBuffer* buffer = new PipelineBuffer();
    pipeline->addBuffer(buffer);
    return buffer;
}
