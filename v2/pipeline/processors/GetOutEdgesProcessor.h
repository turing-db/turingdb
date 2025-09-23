#pragma once

#include <memory>

#include "Processor.h"

namespace db {
class GetOutEdgesChunkWriter;
}

namespace db::v2 {

class PipelineBuffer;

class GetOutEdgesProcessor : public Processor {
public:
    static GetOutEdgesProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;

    void reset() override;

    void execute() override;

private:
    PipelineBuffer* _inNodeIDs {nullptr};
    PipelineBuffer* _outIndices {nullptr};
    PipelineBuffer* _outEdgeIDs {nullptr};
    PipelineBuffer* _outTargetNodes {nullptr};
    PipelineBuffer* _outEdgeTypes {nullptr};
    std::unique_ptr<GetOutEdgesChunkWriter> _it;

    GetOutEdgesProcessor();
    ~GetOutEdgesProcessor();
};

}
