#pragma once

#include <cstdint>
#include <ranges>
#include <span>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

namespace db {

class Column;
class Dataframe;

class OrderByProcessor final : public Processor {
public:
    struct OrderByKey {
        Column* _col {nullptr};
        bool _asc {true};
    };

    struct TieRange {
        size_t start {0};
        size_t size {0};
    };

    using OrderByKeys = std::vector<OrderByKey>;
    using Indices = std::vector<size_t>;
    using TieRanges = std::vector<TieRange>;

    OrderByProcessor(const OrderByProcessor&) = delete;
    OrderByProcessor(OrderByProcessor&&) = delete;
    OrderByProcessor& operator=(const OrderByProcessor&) = delete;
    OrderByProcessor& operator=(OrderByProcessor&&) = delete;

    static OrderByProcessor* create(PipelineV2* pipeline, std::span<OrderByKey> keys);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    template <std::ranges::random_access_range Rg>
    static void addTieRanges(TieRanges& tieRanges, const Rg& rg, size_t start = 0);

private:
    OrderByProcessor();
    ~OrderByProcessor() final;

    enum class State : uint8_t {
        SORT_INCOMING = 0,
        MERGE_SORTED_RUNS,

        STATE_SPACE_SIZE
    };

    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    OrderByKeys _orderedKeys;

    Indices _indices;

    TieRanges _tieRanges;

    State _state {State::SORT_INCOMING};

    void subsort();
};

}
