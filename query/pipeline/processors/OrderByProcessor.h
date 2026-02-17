#pragma once

#include <span>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class Column;

class OrderByProcessor final : public Processor {
public:
    struct OrderByKey {
        Column* _col {nullptr};
        bool _asc {true};
    };

    using OrderByKeys = std::vector<OrderByKey>;
    using Indices = std::vector<size_t>;

    static OrderByProcessor* create(PipelineV2* pipeline, std::span<OrderByKey> keys);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

private:
    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    OrderByKeys _orderedKeys;

    Indices _indices;

    OrderByProcessor();
    ~OrderByProcessor();
};

}
