#pragma once

#include "columns/Block.h"

namespace db::v2 {

class Processor;
class PipelineV2;

class PipelineBuffer {
public:
    friend PipelineV2;

    Processor* getSource() const { return _source; }
    Processor* getConsumer() const { return _consumer; }

    bool hasData() const { return _hasData; }
    bool canWriteData() const { return !_hasData; }

    Block& getBlock() { return _block; }
    const Block& getBlock() const { return _block; }

    void setSource(Processor* source) { _source = source; }
    void setConsumer(Processor* consumer) { _consumer = consumer; }

    void consume() { _hasData = false; }
    void writeData() { _hasData = true; }

    static PipelineBuffer* create(PipelineV2* pipeline);

private:
    Processor* _source {nullptr};
    Processor* _consumer {nullptr};
    Block _block;
    bool _hasData {false};

    PipelineBuffer() = default;
    ~PipelineBuffer() = default;
};

}
