#pragma once

#include <memory>

#include "Processor.h"

#include "PipelineInterface.h"

namespace db {
class GetOutEdgesChunkWriter;
}

namespace db::v2 {

class PipelinePort;

class GetOutEdgesProcessor : public Processor {
public:
    static GetOutEdgesProcessor* create(PipelineV2* pipeline);

    std::string_view getName() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    const PipelineInputInterface& inNodeIDs() const { return _inNodeIDs; }
    const PipelineOutputInterface& outEdges() const { return _outEdges; }

private:
    PipelineInputInterface _inNodeIDs;
    PipelineOutputInterface _outEdges;
    std::unique_ptr<GetOutEdgesChunkWriter> _it;

    GetOutEdgesProcessor();
    ~GetOutEdgesProcessor();
};

}
