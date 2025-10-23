#pragma once

#include "PipelineBlock.h"

namespace db::v2 {

class PipelinePort;
class PipelineV2;

class PipelineBuffer {
public:
    friend PipelineV2;

    PipelinePort* getSource() const { return _source; }
    PipelinePort* getConsumer() const { return _consumer; }

    bool hasData() const { return _hasData; }

    PipelineBlock& getBlock() { return _block; }
    const PipelineBlock& getBlock() const { return _block; }

    void setSource(PipelinePort* source) { _source = source; }
    void setConsumer(PipelinePort* consumer) { _consumer = consumer; }

    void consume() { _hasData = false; }
    void writeData() { _hasData = true; }

    static PipelineBuffer* create(PipelineV2* pipeline);

private:
    PipelinePort* _source {nullptr};
    PipelinePort* _consumer {nullptr};
    PipelineBlock _block;
    bool _hasData {false};

    PipelineBuffer() = default;
    ~PipelineBuffer() = default;
};

}
