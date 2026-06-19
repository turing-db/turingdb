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

    // NOTE: Double by default, do we care about overflows?
    AvgType _sum {0.0};
    size_t _count {0};

    /// Column tag and pointer for the column being averaged
    ColumnTag _colTag;
    const Column* _col {nullptr};

    /// Column to store the numeric value of the avg result
    ColumnConst<AvgType>* _avgColumn {nullptr};

    AvgProcessor();
    ~AvgProcessor() final;
};

}
