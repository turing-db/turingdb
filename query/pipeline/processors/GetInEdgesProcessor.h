#pragma once

#include <memory>

#include "Processor.h"

#include "interfaces/PipelineNodeInputInterface.h"
#include "interfaces/PipelineEdgeOutputInterface.h"

namespace db {
class GetInEdgesChunkWriter;
}

namespace db::v2 {

class GetInEdgesProcessor : public Processor {
public:
    static GetInEdgesProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeInputInterface& inNodeIDs() { return _inNodeIDs; }
    PipelineEdgeOutputInterface& outEdges() { return _outEdges; }

private:
    PipelineNodeInputInterface _inNodeIDs;
    PipelineEdgeOutputInterface _outEdges;
    std::unique_ptr<GetInEdgesChunkWriter> _it;

    GetInEdgesProcessor();
    ~GetInEdgesProcessor();
};

}
