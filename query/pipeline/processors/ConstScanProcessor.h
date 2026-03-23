#pragma once

#include <span>

#include "Processor.h"

#include "ID.h"
#include "columns/ColumnIDs.h"
#include "interfaces/PipelineNodeOutputInterface.h"

namespace db {

class ConstScanProcessor : public Processor {
public:
    static ConstScanProcessor* create(PipelineV2* pipeline,
                                      std::span<const NodeID> nodeIDs);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeOutputInterface& outNodeIDs() { return _outNodeIDs; }

private:
    std::span<const NodeID> _nodeIDs;
    PipelineNodeOutputInterface _outNodeIDs;

    ColumnNodeIDs* _outCol {nullptr};
    size_t _offset {0};

    ConstScanProcessor(std::span<const NodeID> nodeIDs);
    ~ConstScanProcessor();
};

}
