#pragma once

#include "PipelineBuffer.h"

namespace db::v2 {

class PipelineV2;
class Processor;
class PipelineBuffer;

class PipelinePort {
public:
    friend PipelineV2;

    Processor* getProcessor() const { return _processor; }

    PipelinePort* getConnectedPort() const { return _connectedPort; }

    PipelineBuffer* getBuffer() const { return _buffer; }

    bool hasData() const {
        return _connectedPort ? _buffer->hasData() : false;
    }

    bool canWriteData() const {
        return _connectedPort ? !_buffer->hasData() : true;
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

    void connectTo(PipelinePort* other, PipelineBuffer* buffer);

    static PipelinePort* create(PipelineV2* pipeline, Processor* processor);

private:
    Processor* _processor {nullptr};
    PipelinePort* _connectedPort {nullptr};
    PipelineBuffer* _buffer {nullptr};

    PipelinePort(Processor* processor)
        : _processor(processor)
    {
    }

    ~PipelinePort() = default;
};

}
