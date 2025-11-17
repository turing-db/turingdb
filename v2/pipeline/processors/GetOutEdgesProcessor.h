#pragma once

#include <memory>

#include "Processor.h"

#include "interfaces/PipelineNodeInputInterface.h"
#include "interfaces/PipelineEdgeOutputInterface.h"

namespace db {
class GetOutEdgesChunkWriter;
}

namespace db::v2 {

class GetOutEdgesProcessor : public Processor {
public:
    static GetOutEdgesProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeInputInterface& inNodeIDs() { return _inNodeIDs; }
    PipelineEdgeOutputInterface& outEdges() { return _outEdges; }

private:
    PipelineNodeInputInterface _inNodeIDs;
    PipelineEdgeOutputInterface _outEdges;
    std::unique_ptr<GetOutEdgesChunkWriter> _it;

    GetOutEdgesProcessor();
    ~GetOutEdgesProcessor();
};

}
