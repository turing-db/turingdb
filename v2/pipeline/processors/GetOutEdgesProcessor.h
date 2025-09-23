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

    PipelinePort* inNodeIDs() const { return _inNodeIDs; }
    PipelinePort* outIndices() const { return _outIndices; }
    PipelinePort* outEdgeIDs() const { return _outEdgeIDs; }
    PipelinePort* outTargetNodes() const { return _outTargetNodes; }
    PipelinePort* outEdgeTypes() const { return _outEdgeTypes; }

private:
    PipelinePort* _inNodeIDs {nullptr};
    PipelinePort* _outIndices {nullptr};
    PipelinePort* _outEdgeIDs {nullptr};
    PipelinePort* _outTargetNodes {nullptr};
    PipelinePort* _outEdgeTypes {nullptr};
    std::unique_ptr<GetOutEdgesChunkWriter> _it;

    GetOutEdgesProcessor();
    ~GetOutEdgesProcessor();
};

}
