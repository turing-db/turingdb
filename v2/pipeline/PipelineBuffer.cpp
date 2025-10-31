#include "PipelineBuffer.h"

#include "dataframe/Dataframe.h"

#include "PipelineV2.h"

using namespace db::v2;

PipelineBuffer::PipelineBuffer()
    : _dataframe(std::make_unique<Dataframe>())
{
}

PipelineBuffer::~PipelineBuffer() {
}

PipelineBuffer* PipelineBuffer::create(PipelineV2* pipeline) {
    PipelineBuffer* buffer = new PipelineBuffer();
    pipeline->addBuffer(buffer);
    return buffer;
}
