#pragma once

#include <span>
#include <vector>

#include "Processor.h"

#include "interfaces/PipelineValuesOutputInterface.h"

#include "columns/ColumnVector.h"

namespace db {

template <typename T>
class ConstScanProcessor final : public Processor {
public:
    using ColumnValues = ColumnVector<T>;

    static ConstScanProcessor<T>* create(PipelineV2* pipeline, std::span<const T> values);

    std::string describe() const final;

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    PipelineValuesOutputInterface& output() { return _output; }

private:
    PipelineValuesOutputInterface _output;

    std::span<const T> _values;
    std::vector<T> _sortedValues;

    ColumnValues* _outCol {nullptr};
    size_t _offset {0};

    explicit ConstScanProcessor(std::span<const T> values);
    ~ConstScanProcessor() final;
};

}
