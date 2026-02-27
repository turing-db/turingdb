#pragma once

#include <memory>
#include <stdint.h>

#include "Processor.h"

#include "interfaces/PipelineNodeInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnIndices.h"

namespace db {

class LocalMemory;
class GetInEdgesChunkWriter;

class BFSExpandInEdgesProcessor : public Processor {
public:
    static BFSExpandInEdgesProcessor* create(PipelineV2* pipeline,
                                             LocalMemory* mem,
                                             int64_t minHops,
                                             int64_t maxHops);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    void setOutputPathsColumn(NamedColumn* paths) { _outputPaths = paths; }
    void setOutputSourcesColumn(NamedColumn* sourceNodes) { _outputSources = sourceNodes; }
    void setOutputIndicesColumn(ColumnIndices* indices) { _outputIndices = indices; }

    NamedColumn* getOutputSourcesColumn() const { return _outputSources; }
    NamedColumn* getOutputPathsColumn() const { return _outputPaths; }

private:
    BFSExpandInEdgesProcessor(LocalMemory* mem,
                              int64_t minHops,
                              int64_t maxHops);
    ~BFSExpandInEdgesProcessor() override;

    LocalMemory* _mem {nullptr};
    int64_t _minHops {0};
    int64_t _maxHops {0};

    PipelineNodeInputInterface _input;
    PipelineBlockOutputInterface _output;

    ColumnNodeIDs* _inputTargets {nullptr};
    NamedColumn* _outputSources {nullptr};
    NamedColumn* _outputPaths {nullptr};
    ColumnIndices* _outputIndices {nullptr};

    ColumnNodeIDs* _bfsTargets {nullptr};
    ColumnEdgeIDs* _bfsEdges {nullptr};
    ColumnNodeIDs* _bfsIntermediates {nullptr};
    ColumnIndices* _bfsIndices {nullptr};

    std::unique_ptr<GetInEdgesChunkWriter> _bfsWriter;
};

}
