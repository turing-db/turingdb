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
class GetEdgesChunkWriter;

class BFSExpandEdgesProcessor : public Processor {
public:
    static BFSExpandEdgesProcessor* create(PipelineV2* pipeline,
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
    void setOutputTargetsColumn(NamedColumn* targetNodes) { _outputTargets = targetNodes; }
    void setOutputIndicesColumn(ColumnIndices* indices) { _outputIndices = indices; }

    NamedColumn* getOutputTargetsColumn() const { return _outputTargets; }
    NamedColumn* getOutputPathsColumn() const { return _outputPaths; }

private:
    BFSExpandEdgesProcessor(LocalMemory* mem,
                            int64_t minHops,
                            int64_t maxHops);
    ~BFSExpandEdgesProcessor() override;

    LocalMemory* _mem {nullptr};
    int64_t _minHops {0};
    int64_t _maxHops {0};

    PipelineNodeInputInterface _input;
    PipelineBlockOutputInterface _output;

    ColumnNodeIDs* _inputSources {nullptr};
    NamedColumn* _outputTargets {nullptr};
    NamedColumn* _outputPaths {nullptr};
    ColumnIndices* _outputIndices {nullptr};

    ColumnNodeIDs* _bfsNodes {nullptr};
    ColumnEdgeIDs* _bfsEdges {nullptr};
    ColumnNodeIDs* _bfsIntermediates {nullptr};
    ColumnIndices* _bfsIndices {nullptr};

    std::unique_ptr<GetEdgesChunkWriter> _bfsWriter;
};

}
