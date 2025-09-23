#pragma once

#include <memory>

#include "Processor.h"

namespace db {
class GetOutEdgesChunkWriter;
}

namespace db::v2 {

class PipelinePort;

class GetOutEdgesProcessor : public Processor {
public:
    static GetOutEdgesProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineInputPort* inNodeIDs() const { return _inNodeIDs; }
    PipelineOutputPort* outIndices() const { return _outIndices; }
    PipelineOutputPort* outEdgeIDs() const { return _outEdgeIDs; }
    PipelineOutputPort* outTargetNodes() const { return _outTargetNodes; }
    PipelineOutputPort* outEdgeTypes() const { return _outEdgeTypes; }

private:
    PipelineInputPort* _inNodeIDs {nullptr};
    PipelineOutputPort* _outIndices {nullptr};
    PipelineOutputPort* _outEdgeIDs {nullptr};
    PipelineOutputPort* _outTargetNodes {nullptr};
    PipelineOutputPort* _outEdgeTypes {nullptr};
    std::unique_ptr<GetOutEdgesChunkWriter> _it;

    GetOutEdgesProcessor();
    ~GetOutEdgesProcessor();
};

}
