#pragma once

#include <memory>

namespace db {
class Dataframe;
}

namespace db::v2 {

class PipelinePort;
class PipelineV2;

class PipelineBuffer {
public:
    friend PipelineV2;

    PipelinePort* getSource() const { return _source; }
    PipelinePort* getConsumer() const { return _consumer; }

    bool hasData() const { return _hasData; }

    Dataframe* getDataframe() { return _dataframe.get(); }
    const Dataframe* getDataframe() const { return _dataframe.get(); }

    void setSource(PipelinePort* source) { _source = source; }
    void setConsumer(PipelinePort* consumer) { _consumer = consumer; }

    void consume() { _hasData = false; }
    void writeData() { _hasData = true; }

    static PipelineBuffer* create(PipelineV2* pipeline);

private:
    PipelinePort* _source {nullptr};
    PipelinePort* _consumer {nullptr};
    bool _hasData {false};
    std::unique_ptr<Dataframe> _dataframe;

    PipelineBuffer();
    ~PipelineBuffer();
};

}
