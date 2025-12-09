#pragma once

#include "PipelineBuffer.h"

namespace db::v2 {

class PipelineV2;
class Processor;
class PipelineBuffer;
class PipelineInputPort;
class PipelineOutputPort;

class PipelinePort {
public:
    friend PipelineV2;

    Processor* getProcessor() const { return _processor; }
    PipelineBuffer* getBuffer() const { return _buffer; }

    bool isOpen() const { return _open; }
    bool isClosed() const { return !_open; }

    bool hasData() const {
        return _connectedPort ? _buffer->hasData() : false;
    }

    void setNeedsData(bool needsData) {
        _needsData = needsData;
    }

    bool needsData() const {
        return _connectedPort ? _needsData : false;
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

protected:
    Processor* _processor {nullptr};
    PipelinePort* _connectedPort {nullptr};
    PipelineBuffer* _buffer {nullptr};
    bool _open {true};
    bool _needsData {true};

    explicit PipelinePort(Processor* processor)
        : _processor(processor)
    {
    }

    ~PipelinePort() = default;

    void postCreate(PipelineV2* pipeline);
};

class PipelineInputPort : public PipelinePort {
public:
    friend PipelineOutputPort;

    PipelineOutputPort* getConnectedPort() const { return (PipelineOutputPort*)_connectedPort; }

    void connectTo(PipelineOutputPort* other);

    void consume() {
        if (_connectedPort) {
            _buffer->consume();
        }
    }

    bool hasData() const {
        return _connectedPort ? _buffer->hasData() : false;
    }

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

    PipelineInputPort* getConnectedPort() const { return (PipelineInputPort*)_connectedPort; }

    void connectTo(PipelineInputPort* other);

    void writeData() {
        if (_connectedPort) {
            _connectedPort->getBuffer()->writeData();
        }
    }

    bool canWriteData() const {
        return _connectedPort ? !_buffer->hasData() : true;
    }

    static PipelineOutputPort* create(PipelineV2* pipeline, Processor* processor);

private:
    PipelineOutputPort(Processor* processor)
        : PipelinePort(processor)
    {
    }

    ~PipelineOutputPort() = default;
};

}
