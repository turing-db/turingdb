#pragma once

#include "Processor.h"

#include "columns/ColumnConst.h"
#include "metadata/PropertyType.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineValueOutputInterface.h"

namespace db {

class CountProcessor : public Processor {
public:
    using CountType = types::UInt64::Primitive;

    static CountProcessor* create(PipelineV2* pipeline, ColumnTag colTag);

    std::string describe() const override;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineValueOutputInterface& output() { return _output; }

    void prepare(ExecutionContext* ctxt) override;
    void reset() override;
    void execute() override;

private:
    PipelineBlockInputInterface _input;
    PipelineValueOutputInterface _output;
    CountType _countRunning {0};
    ColumnTag _colTag;
    const Column* _col {nullptr};
    ColumnConst<CountType>* _countColumn {nullptr};

    CountProcessor();
    ~CountProcessor();
};

// Methods such as @ref Dataframe::getLogicalRowCount return a size_t; ensure types are
// compatible.
static_assert(std::numeric_limits<size_t>::max()
              == std::numeric_limits<CountProcessor::CountType>::max());
}
