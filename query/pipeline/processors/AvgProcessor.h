#pragma once

#include "Processor.h"

#include "columns/ColumnConst.h"
#include "metadata/PropertyType.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class AvgProcessor final : public Processor {
public:
    using AvgType = types::Double::Primitive;

    static AvgProcessor* create(PipelineV2* pipeline, ColumnTag colTag);

    std::string describe() const final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValueOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

private:
    PipelineBlockInputInterface _input;
    PipelineValueOutputInterface _output;

    // TODO @cyrus: For integer inputs, accumulating as double avoids the need to track a
    // separate int64 sum, but introduces floating-point precision loss for very large integer
    // values. Decide whether to use a separate int64 accumulator for integer-typed columns
    // and only convert to double at division time.
    AvgType _sumRunning {0.0};
    size_t _countRunning {0};

    /// Column tag and pointer for the column being averaged
    ColumnTag _colTag;
    const Column* _col {nullptr};

    /// Column to store the numeric value of the avg result
    ColumnConst<AvgType>* _avgColumn {nullptr};

    AvgProcessor();
    ~AvgProcessor() final;
};

}
