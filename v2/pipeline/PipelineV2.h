#pragma once

#include <vector>
#include <unordered_set>

namespace db::v2 {

class Processor;
class PipelinePort;
class PipelineBuffer;

class PipelineV2 {
public:
    friend Processor;
    friend PipelinePort;
    friend PipelineBuffer;
    using Processors = std::vector<Processor*>;
    using SourcesSet = std::unordered_set<Processor*>;
    using Buffers = std::vector<PipelineBuffer*>;
    using Ports = std::vector<PipelinePort*>;

    PipelineV2();
    ~PipelineV2();

    const SourcesSet& sources() const { return _sources; }

    const Processors& processors() const { return _processors; }

private:
    Processors _processors;
    Buffers _buffers;
    Ports _ports;
    SourcesSet _sources;

    void addProcessor(Processor* processor);
    void addPort(PipelinePort* port);
    void addBuffer(PipelineBuffer* buffer);
};

}
