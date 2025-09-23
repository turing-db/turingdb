#include "PipelinePort.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"

using namespace db::v2;

PipelinePort* PipelinePort::create(PipelineV2* pipeline, Processor* processor) {
    PipelinePort* port = new PipelinePort(processor);
    pipeline->addPort(port);
    return port;
}

void PipelinePort::connectTo(PipelinePort* other, PipelineBuffer* buffer) {
    _connectedPort = other;
    _buffer = buffer;
    other->_connectedPort = this;
    other->_buffer = buffer;

    buffer->setSource(this);
    buffer->setConsumer(other);
}
