#pragma once

#include "PipelineBuffer.h"

namespace db::v2 {

class PipelineV2;
class Processor;
class PipelineBuffer;
class PipelineOutputPort;

class PipelinePort {
public:
    friend PipelineV2;

    Processor* getProcessor() const { return _processor; }
    PipelinePort* getConnectedPort() const { return _connectedPort; }
    PipelineBuffer* getBuffer() const { return _buffer; }

    bool isConnected() const { return _connectedPort != nullptr; }

    bool isOpen() const { return _open; }
    bool isClosed() const { return !_open; }

    bool hasData() const {
        return _connectedPort ? _buffer->hasData() : false;
    }

    bool canWriteData() const {
        return _connectedPort ? !_buffer->hasData() : true;
    }

    void close() {
        _open = false;
        if (_connectedPort) {
            _connectedPort->_open = false;
        }
    }

    void consume() {
        if (_connectedPort) {
            _buffer->consume();
        }
    }

    void writeData() {
        if (_connectedPort) {
            _buffer->writeData();
        }
    }

protected:
    Processor* _processor {nullptr};
    PipelinePort* _connectedPort {nullptr};
    PipelineBuffer* _buffer {nullptr};
    bool _open {true};

    PipelinePort(Processor* processor)
        : _processor(processor)
    {}

    ~PipelinePort() = default;

    void postCreate(PipelineV2* pipeline);
};

class PipelineInputPort : public PipelinePort {
public:
    friend PipelineOutputPort;

    void connectTo(PipelineOutputPort* other);

    static PipelineInputPort* create(PipelineV2* pipeline, Processor* processor);

private:
    PipelineInputPort(Processor* processor)
        : PipelinePort(processor)
    {}

    ~PipelineInputPort() = default;
};

class PipelineOutputPort : public PipelinePort {
public:
    friend PipelineInputPort;

    void connectTo(PipelineInputPort* other);

    static PipelineOutputPort* create(PipelineV2* pipeline, Processor* processor);

private:
    PipelineOutputPort(Processor* processor)
        : PipelinePort(processor)
    {}

    ~PipelineOutputPort() = default;
};

}
