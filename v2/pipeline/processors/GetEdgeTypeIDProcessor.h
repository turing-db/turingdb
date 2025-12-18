#pragma once

#include "Processor.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnVector.h"
#include "ID.h"

#include "interfaces/PipelineValuesOutputInterface.h"
#include "interfaces/PipelineEdgeInputInterface.h"

namespace db::v2 {

class PipelineV2;

class GetEdgeTypeIDProcessor final : public Processor {
public:
    using EdgeTypeIDs = ColumnVector<EdgeTypeID>;

    static GetEdgeTypeIDProcessor* create(PipelineV2* pipeline);

    std::string describe() const final;

    void prepare(ExecutionContext*) final;
    void reset() final;
    void execute() final;

    PipelineEdgeInputInterface& input() { return _input; }
    PipelineValuesOutputInterface& output() { return _output; }

private:
    PipelineEdgeInputInterface _input;
    PipelineValuesOutputInterface _output;
    ColumnEdgeIDs* _edgeIDs {nullptr};
    EdgeTypeIDs* _edgeTypeIDs {nullptr};

    GetEdgeTypeIDProcessor();
    ~GetEdgeTypeIDProcessor();
};

}
