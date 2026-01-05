#include "PipelinePort.h"

#include "PipelineV2.h"
#include "PipelineBuffer.h"

using namespace db::v2;

void PipelinePort::postCreate(PipelineV2* pipeline) {
    pipeline->addPort(this);
}

PipelineInputPort* PipelineInputPort::create(PipelineV2* pipeline, Processor* processor) {
    PipelineInputPort* port = new PipelineInputPort(processor);
    port->postCreate(pipeline);
    return port;
}

PipelineOutputPort* PipelineOutputPort::create(PipelineV2* pipeline, Processor* processor) {
    PipelineOutputPort* port = new PipelineOutputPort(processor);

    port->_buffer = PipelineBuffer::create(pipeline);

    port->postCreate(pipeline);
    return port;
}

void PipelineOutputPort::connectTo(PipelineInputPort* other) {
    _connectedPort = other;
    other->_connectedPort = this;
    other->_buffer = _buffer;

    _buffer->setSource(this);
    _buffer->setConsumer(other);
}

void PipelineInputPort::connectTo(PipelineOutputPort* other) {
    _connectedPort = other;
    other->_connectedPort = this;
    _buffer = other->_buffer;

    _buffer->setSource(this);
    _buffer->setConsumer(other);
}
