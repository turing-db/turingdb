#pragma once

#include <vector>
#include <unordered_set>

#include "dataframe/DataframeManager.h"

namespace db {

class Processor;
class PipelinePort;
class PipelineBuffer;
class ExprProgram;
class PredicateProgram;

class PipelineV2 {
public:
    friend Processor;
    friend PipelinePort;
    friend PipelineBuffer;
    friend ExprProgram;
    friend PredicateProgram;
    using Processors = std::vector<Processor*>;
    using SourcesSet = std::vector<Processor*>;
    using Buffers = std::vector<PipelineBuffer*>;
    using Ports = std::vector<PipelinePort*>;
    using ExprPrograms = std::vector<ExprProgram*>;

    PipelineV2();
    ~PipelineV2();

    DataframeManager* getDataframeManager() { return &_dfMan; }

    const SourcesSet& sources() const { return _sources; }

    const Processors& processors() const { return _processors; }

    void clear();

private:
    Processors _processors;
    Buffers _buffers;
    Ports _ports;
    SourcesSet _sources;
    ExprPrograms _exprProgs;
    DataframeManager _dfMan;

    void addProcessor(Processor* processor);
    void addPort(PipelinePort* port);
    void addBuffer(PipelineBuffer* buffer);
    void addExprProgram(ExprProgram* prog);
};

}
