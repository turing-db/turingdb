#pragma once

#include <ranges>
#include <span>
#include <stdint.h>

#include "Processor.h"

#include "interfaces/PipelineBlockInputInterface.h"
#include "interfaces/PipelineBlockOutputInterface.h"

#include "columns/ColumnVector.h"
#include "dataframe/Dataframe.h"

namespace db {

class Column;
class Dataframe;

/**
 * @brief Processor to perform sorting based on ORDER BY clauses.
 * @detail Three-stage algorithm:
 *            1. (On @ref execute, whilst @ref _input is not closed)
 *               Sort incoming dataframe by specified keys. Store sorted run into next
 *               available slot in @ref _memory.
 *
 *            2. (Upon first call to @ref execute after input is closed)
 *               Sort all sorted runs in @ref _memory, to achieve a total ordering of all
 *               inputs.
 *
 *            3. (From second call to @ref execute after input is closed)
 *               Emit rows in the total order achieved in 2. to @ref _output. Emit at most
 *               one chunk at a time. Repeat until all rows from memory have been emitted.
 *
 * All sorting is done using a "late materialisation" strategy, where only indices are
 * sorted, until we need the rows to be materialised (i.e. when storing to memory, when
 * emitting a chunk), at which point we project the new indices-defined order onto the
 * destination dataframe.
 */
class OrderByProcessor final : public Processor {
public:
    /// Defines a column and sorting order which is to be sorted.
    struct OrderByKey {
        ColumnTag _col;
        bool _asc {true};
    };

    /// Defines a range [_start, _start + _size) of some column of equal value.
    struct TieRange {
        size_t _start {0};
        size_t _size {0};
    };

    /// Defines a sorted run over indexes [_start, _start + _size).
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

    static OrderByProcessor* create(PipelineV2* pipeline, std::span<const OrderByKey> keys);

    void prepare(ExecutionContext* ctxt) final;
    void reset() final;
    void execute() final;

    std::string describe() const final;

    PipelineBlockInputInterface& input() { return _input; }
    PipelineBlockOutputInterface& output() { return _output; }

    Dataframe& memory() { return _memory; }

    /**
     * @ref Helper function to find subranges, S, [l, r) in a a range, R, where
     * R[i] = R[j] for l <= i <= j < r. Appends all such S found in @param rg to
     * @param tieRanges.
     */
    template <std::ranges::random_access_range Rg>
    static void addTieRanges(TieRanges& tieRanges, const Rg& rg, size_t start = 0);

    void setIndicesCol(ColumnVector<size_t>* indices) { _indices = indices; }

private:
    /// 3-state logic for each phase of the sorting algorithm
    enum class State : uint8_t {
        SORT_INCOMING = 0,
        MERGE_SORTED_RUNS,
        EMIT_FROM_MEMORY,

        STATE_SPACE_SIZE
    };

    OrderByProcessor();
    ~OrderByProcessor() final;

    PipelineBlockInputInterface _input;
    PipelineBlockOutputInterface _output;

    /// Columns to sort by, and their direction, in decreasing order of precedence.
    OrderByKeys _orderedKeys;

    /// Indices used in late-materialisation of all stages of sorting.
    Indices* _indices {nullptr};

    /// Ranges of equal values, used in @ref subsort.
    TieRanges _tieRanges;

    /// Memory to store sorted runs.
    Dataframe _memory;

    /// Indexes of sorted runs stored in @ref _memory.
    SortedRuns _sortedRuns;

    /**
     * @brief Used to find the next slot in @ref _memory to:
     * - store the next sorted run (if @ref _state == State::SORT_INCOMING),
     * - emit from (if @ref _state == State::EMIT_FROM_MEMORY).
     */
    size_t _nextMemoryStart {0};

    State _state {State::SORT_INCOMING};

    /**
     * @brief Column-oriented dataframe-sorting algorithm used in @ref
     * State::SORT_INCOMING.
     * @detail for an ordered sequence of order-keys, $k_1, k_2, k_3, ..., k_i$
     * 1. Sort with respect to values in $k_1$
     *    for $2\le j < i$:
     * 2. Check for contiguous runs of rows in column $k_{j}$ where there are ties on the
     *    same value in $k_{j-1}$, call these runs $R$.
     * 3. Sort the rows in each run $r \in R$ with respect to $k_{j}$.
     */
    void subsort();

    /**
     * @brief Projects values from @param src to @param dst according to the mapping
     * contained in @ref _indices.
     */
    void project(const Column* src, Column* dst, size_t numRows, size_t fromSrcRow = 0, size_t fromDstRow = 0);

    /// Projects a sorted run of the data in @ref _input to @ref _memory.
    void memorise();

    /// Sorts @ref _indices via a merge of sorted runs in @ref _memory.
    void merge();
};

}
