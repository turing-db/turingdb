#pragma once

#include <vector>
#include <unordered_set>

namespace db::v2 {

class Processor;
class PipelineBuffer;

class PipelineV2 {
public:
    friend Processor;
    friend PipelineBuffer;
    using Processors = std::vector<Processor*>;
    using SourcesSet = std::unordered_set<Processor*>;
    using Buffers = std::vector<PipelineBuffer*>;

    PipelineV2();
    ~PipelineV2();

    const SourcesSet& sources() const { return _sources; }

    const Processors& processors() const { return _processors; }

private:
    Processors _processors;
    Buffers _buffers;
    SourcesSet _sources;

    void addProcessor(Processor* processor);
    void addBuffer(PipelineBuffer* buffer);
};

}
