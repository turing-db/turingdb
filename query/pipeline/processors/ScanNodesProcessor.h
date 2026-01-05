#pragma once

#include <memory>

#include "Processor.h"

#include "interfaces/PipelineNodeOutputInterface.h"

namespace db {
class ScanNodesChunkWriter;
}

namespace db {

class ScanNodesProcessor : public Processor {
public:
    static ScanNodesProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeOutputInterface& outNodeIDs() { return _outNodeIDs; }

private:
    PipelineNodeOutputInterface _outNodeIDs;
    std::unique_ptr<ScanNodesChunkWriter> _it;

    ScanNodesProcessor();
    ~ScanNodesProcessor();
};

}
