#pragma once

#include <memory>

#include "Processor.h"

namespace db {
class ScanNodesChunkWriter;
}

namespace db::v2 {

class PipelineBuffer;

class ScanNodesProcessor : public Processor {
public:
    static ScanNodesProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;

    void reset() override;

    void execute() override;

private:
    PipelineBuffer* _outNodeIDs {nullptr};
    std::unique_ptr<ScanNodesChunkWriter> _it;

    ScanNodesProcessor();
    ~ScanNodesProcessor();
};


}
