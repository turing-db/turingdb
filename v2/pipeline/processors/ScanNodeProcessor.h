#pragma once

#include <memory>

#include "Processor.h"

namespace db {
class ScanNodesChunkWriter;
}

namespace db::v2 {

class PipelineBuffer;

class ScanNodeProcessor : public Processor {
public:
    static ScanNodeProcessor* create(PipelineV2* pipeline);

    void prepare(ExecutionContext* ctxt) override;

    void reset() override;

    void execute() override;

private:
    PipelineBuffer* _outNodeIDs {nullptr};
    std::unique_ptr<ScanNodesChunkWriter> _it;

    ScanNodeProcessor();
    ~ScanNodeProcessor();
};


}
