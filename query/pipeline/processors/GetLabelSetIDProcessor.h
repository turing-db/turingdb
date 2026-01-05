#pragma once

#include "Processor.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"

#include "interfaces/PipelineValuesOutputInterface.h"
#include "interfaces/PipelineNodeInputInterface.h"

namespace db::v2 {

class GetLabelSetIDProcessor : public Processor {
public:
    using LabelSetIDs = ColumnVector<LabelSetID>;

    static GetLabelSetIDProcessor* create(PipelineV2* pipeline);

    std::string describe() const override;

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

    PipelineNodeInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

protected:
    PipelineNodeInputInterface _input;
    PipelineValuesOutputInterface _output;
    ColumnNodeIDs* _nodeIDs {nullptr};
    LabelSetIDs* _labelsetIDs {nullptr};

    GetLabelSetIDProcessor();
    ~GetLabelSetIDProcessor();
};

}
