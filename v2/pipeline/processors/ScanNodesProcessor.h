#pragma once

#include <memory>

#include "Processor.h"

namespace db {
class ScanNodesChunkWriter;
}

namespace db::v2 {

class PipelinePort;

class ScanNodesProcessor : public Processor {
public:
    static ScanNodesProcessor* create(PipelineV2* pipeline);

    std::string_view getName() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineOutputPort* outNodeIDs() const { return _outNodeIDs; }

private:
    PipelineOutputPort* _outNodeIDs {nullptr};
    std::unique_ptr<ScanNodesChunkWriter> _it;

    ScanNodesProcessor();
    ~ScanNodesProcessor();
};


}
