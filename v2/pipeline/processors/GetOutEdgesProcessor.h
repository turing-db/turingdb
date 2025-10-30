#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

namespace db {
class GetOutEdgesChunkWriter;
}

namespace db::v2 {

class GetOutEdgesProcessor : public Processor {
public:
    static GetOutEdgesProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineInputInterface& inNodeIDs() { return _inNodeIDs; }
    PipelineOutputInterface& outIndices() { return _outIndices; }
    PipelineOutputInterface& outEdgeIDs() { return _outEdgeIDs; }
    PipelineOutputInterface& outTargetNodes() { return _outTargetNodes; }
    PipelineOutputInterface& outEdgeTypes() { return _outEdgeTypes; }

private:
    PipelineInputInterface _inNodeIDs;
    PipelineOutputInterface _outIndices;
    PipelineOutputInterface _outEdgeIDs;
    PipelineOutputInterface _outTargetNodes;
    PipelineOutputInterface _outEdgeTypes;
    std::unique_ptr<GetOutEdgesChunkWriter> _it;

    GetOutEdgesProcessor();
    ~GetOutEdgesProcessor();
};

}
