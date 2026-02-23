#pragma once

#include <cstdint>
#include <ranges>
#include <span>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

namespace db {

class Column;
class Dataframe;

class OrderByProcessor final : public Processor {
public:
    /// Defines a column and sortig order which is to be sorted
    struct OrderByKey {
        ColumnTag _col;
        bool _asc {true};
    };

    /// Defines a subrange [_start, _start + _size) which contains the same value
    struct TieRange {
        size_t _start {0};
        size_t _size {0};
    };

    /// Defines a sorted run over indexes [_start, _start + _size)
    struct SortedRun {
        size_t _start {0};
        size_t _size {0};
    };

    using OrderByKeys = std::vector<OrderByKey>;
    using Indices = ColumnVector<size_t>;
    using TieRanges = std::vector<TieRange>;
    using SortedRuns = std::vector<SortedRun>;

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

    Dataframe& memory() { return _memory; }

    template <std::ranges::random_access_range Rg>
    static void addTieRanges(TieRanges& tieRanges, const Rg& rg, size_t start = 0);

    void setIndicesCol(ColumnVector<size_t>* indices) { _indices = indices; }

private:
    enum class State : uint8_t {
        SORT_INCOMING = 0,
        MERGE_SORTED_RUNS,
        OUTPUT_FROM_MEMORY,

        STATE_SPACE_SIZE
    };

    OrderByProcessor();
    ~OrderByProcessor() final;

    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    OrderByKeys _orderedKeys;

    Indices* _indices {nullptr};

    TieRanges _tieRanges;

    Dataframe _memory;

    SortedRuns _sortedRuns;

    size_t _nextMemoryStart {0};

    State _state {State::SORT_INCOMING};

    void subsort();

    void project(const Column* src, Column* dst, size_t numRows, size_t fromRow = 0);

    void memorise();

    void merge();
};

}
