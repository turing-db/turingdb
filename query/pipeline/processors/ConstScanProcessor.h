#pragma once

#include "Processor.h"

#include "interfaces/PipelineValuesOutputInterface.h"

namespace db {

class Column;

class ConstScanProcessor final : public Processor {
public:
    static ConstScanProcessor* create(PipelineV2* pipeline, Column* values);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValuesOutputInterface& output() { return _output; }

    const Column* values() { return _values; }

private:
    PipelineValuesOutputInterface _output;

    Column* _values;

    size_t _offset {0};

    explicit ConstScanProcessor(Column* values);
    ~ConstScanProcessor() final;
};

}
