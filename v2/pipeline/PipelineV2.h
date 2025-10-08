#pragma once

#include <unordered_set>

#include "SmallVector.h"

namespace db::v2 {

class Processor;
class PipelinePort;
class PipelineBuffer;
class MaterializeData;

class PipelineV2 {
public:
    friend Processor;
    friend PipelinePort;
    friend PipelineBuffer;
    friend MaterializeData;
    constexpr static size_t DEFAULT_PROCESSORS_CAPACITY = 16;
    constexpr static size_t DEFAULT_BUFFERS_CAPACITY = 32;
    constexpr static size_t DEFAULT_PORTS_CAPACITY = 32;
    constexpr static size_t DEFAULT_MATERIALIZE_DATAS_CAPACITY = 2;
    using Processors = SmallVector<Processor*, DEFAULT_PROCESSORS_CAPACITY>;
    using SourcesSet = std::unordered_set<Processor*>;
    using Buffers = SmallVector<PipelineBuffer*, DEFAULT_BUFFERS_CAPACITY>;
    using Ports = SmallVector<PipelinePort*, DEFAULT_PORTS_CAPACITY>;
    using MaterializeDatas = SmallVector<MaterializeData*, DEFAULT_MATERIALIZE_DATAS_CAPACITY>;

    PipelineV2();
    ~PipelineV2();

    const SourcesSet& sources() const { return _sources; }

    const Processors& processors() const { return _processors; }

private:
    Processors _processors;
    Buffers _buffers;
    Ports _ports;
    SourcesSet _sources;
    MaterializeDatas _materializeDatas;

    void addProcessor(Processor* processor);
    void addPort(PipelinePort* port);
    void addBuffer(PipelineBuffer* buffer);
    void addMaterializeData(MaterializeData* data);
};

}
