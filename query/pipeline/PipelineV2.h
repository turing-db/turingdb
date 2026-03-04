#pragma once

#include <vector>

#include "dataframe/DataframeManager.h"

namespace db {

class Processor;
class PipelinePort;
class PipelineBuffer;
class ExprProgram;
class PredicateProgram;
class Dataframe;
class FunctionProgram;

class PipelineV2 {
public:
    friend Processor;
    friend PipelinePort;
    friend PipelineBuffer;
    friend ExprProgram;
    friend PredicateProgram;
    friend FunctionProgram;
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

    void setOutputDataframe(const Dataframe* df) { _outputDataframe = df; }

    const Dataframe* getOutputDataframe() const { return _outputDataframe; }

private:
    Processors _processors;
    Buffers _buffers;
    Ports _ports;
    SourcesSet _sources;
    ExprPrograms _exprProgs;
    DataframeManager _dfMan;
    const Dataframe* _outputDataframe {nullptr};

    void addProcessor(Processor* processor);
    void addPort(PipelinePort* port);
    void addBuffer(PipelineBuffer* buffer);
    void addExprProgram(ExprProgram* prog);
};

}
