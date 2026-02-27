#include "OrderByProcessor.h"

#include <algorithm>
#include <compare>
#include <concepts>
#include <numeric>

#include <range/v3/action/sort.hpp>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/view/transform.hpp>

#include <spdlog/fmt/bundled/format.h>

#include "ExecutionContext.h"
#include "PipelinePort.h"

#include "columns/AllowedKinds.h"
#include "columns/Column.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "dataframe/Dataframe.h"
#include "dataframe/NamedColumn.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

template <typename T>
    requires std::totally_ordered<T>
int compare(const T& a, const T& b) {
    const std::partial_ordering cmp = a <=> b;

    if (cmp == std::partial_ordering::less) {
        return -1;
    }

    if (cmp == std::partial_ordering::greater) {
        return  1;
    }

    if (cmp == std::partial_ordering::equivalent) {
        return 0;
    }

    // FIXME: How do we want to handle non-totally ordered types like double?
    /*(cmp == std::partial_ordering::unordered)*/
    return 0;
}

/// Implements a NULLS LAST ordering on optional types
/// As per: https://neo4j.com/docs/cypher-manual/current/clauses/order-by/#null
template <typename T>
    requires std::totally_ordered<T>
int compare(const std::optional<T>& a, const std::optional<T>& b) {
    // NULL == NULL
    if (!a.has_value() && !b.has_value()) {
        return 0;
    }

    // NON NULL < NULL (makes NULLs appear last)
    if (!b.has_value()) {
        return -1;
    }

    // NULL > NON NULL (makes NULLs appear last)
    if (!a.has_value()) {
        return 1;
    }

    // Otherwise both enganged, ordered normally
    return compare(*a, *b);
}

/**
 * @brief Functor to sort @ref _indices by values in a column, and populate @ref _ranges
 * as tieranges.
 */
class OrderColumn {
public:
    OrderColumn(OrderByProcessor::Indices& indices,
                OrderByProcessor::TieRanges& tieranges,
                bool asc)
        : _indices(indices),
        _ranges(tieranges),
        _asc(asc)
    {
    }

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(const ColumnVector<T>* typed) {
        const std::vector<T>& data = typed->getRaw();
        bioassert(_indices.size() == data.size(),
                  "Indices did not match column dimensions.");

        // Sort the indices by indexing the column
        if (_asc) {
            const auto lt = [&data](size_t i, size_t j) {
                return compare(data[i], data[j]) < 0;
            };
            rg::sort(_indices, lt);
        } else {
            const auto gt = [&data](size_t i, size_t j) {
                return compare(data[i], data[j]) > 0;
            };
            rg::sort(_indices, gt);
        }

        // Get a view of the column with the sorted indices
        const auto reordered = _indices
                               | rv::transform([&](std::size_t i) -> auto& { return data[i]; });

        OrderByProcessor::addTieRanges(_ranges, reordered);
    }

    // ColumnConst is vacuously sorted
    template <typename T>
    void operator()(const ColumnConst<T>*) {}

private:
    OrderByProcessor::Indices& _indices;
    OrderByProcessor::TieRanges& _ranges;
    bool _asc {true};
};

/// Functor to sort @ref _indices by subranges in a column.
class OrderColumnSubrange {
public:
    OrderColumnSubrange(OrderByProcessor::Indices& indices, bool asc)
        : _indices(indices),
          _asc(asc)
    {
    }

    void setStart(size_t start) { _subrangeStart = start; }
    void setEnd(size_t end) { _subrangeEnd = end; }
    size_t start() const { return _subrangeStart; }
    size_t end() const { return _subrangeEnd; }

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(const ColumnVector<T>* typed) {
        const std::vector<T>& data = typed->getRaw();

        const auto beginIt = std::begin(_indices) + _subrangeStart;
        const auto endIt = std::begin(_indices) + _subrangeEnd;

        if (_asc) {
            std::sort(beginIt, endIt,
                      [&data](size_t i, size_t j) { return data[i] < data[j]; });
        } else {
            std::sort(beginIt, endIt,
                      [&data](size_t i, size_t j) { return data[i] > data[j]; });
        }
    }

    // ColumnConst is vacuously sorted
    template <typename T>
    void operator()(const ColumnConst<T>*) {}

private:
    OrderByProcessor::Indices& _indices;
    size_t _subrangeStart {0};
    size_t _subrangeEnd {0};
    bool _asc {true};
};

/**
 * @brief Shrinks the tieranges in @ref _ranges according to the values contained in
 * @ref _typed.
 * @detail For an ordered pair of order-keys $k_1, k_2$, and an array of tieranges, R, for
 * $k_1$, shrinks each range r = [l, r) in R to r' = [l', r') such that l <= l', r' <= r,
 * $k_2$[i] = $k_2$[j] for l' <= i <= j < r', $k_2$[l' - 1] != $k_2$[l'], and
 * $k_2$[r] != $k_2$[r + 1].
 *
 */
class NarrowTieRanges {
public:
    NarrowTieRanges(OrderByProcessor::Indices& indices,
                    OrderByProcessor::TieRanges& ranges)
        : _indices(indices),
        _ranges(ranges)
    {
    }

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(const ColumnVector<T>* typed) {
        const std::vector<T>& data = typed->getRaw();

        // Get a view of the sorted column
        const auto reordered = _indices
                               | rv::transform([&](std::size_t i) -> auto& { return data[i]; });

        // Temporary vector which will contain the new tie-ranges
        OrderByProcessor::TieRanges temp;

        // Add subranges of the subranges in @ref _ranges that are still tied
        for (const auto& [start, size] : _ranges) {
            const size_t end = start + size;

            const auto beginIt = std::begin(reordered) + start;
            const auto endIt = std::begin(reordered) + end;
            const auto tiedRange = rg::subrange(beginIt, endIt);

            OrderByProcessor::addTieRanges(temp, tiedRange, start);
        }

        // Replace the new subranges with the old
        _ranges.swap(temp);
    }

    // ColumnConst does not change tie ranges
    template <typename T>
    void operator()(const ColumnConst<T>*) {}

private:
    OrderByProcessor::Indices& _indices;
    OrderByProcessor::TieRanges& _ranges;
};

/// Functor to project a new ordering defined by @ref _indices on to @ref _res
class ProjectOrder {
public:
    ProjectOrder(Column* result,
                 OrderByProcessor::Indices& indices,
                 size_t numRows,
                 size_t fromSrcRow,
                 size_t fromDstRow)
        : _res(result),
        _indices(indices),
        _numRows(numRows),
        _fromSrcRow(fromSrcRow),
        _fromDstRow(fromDstRow)
    {
    }

    template <typename T>
        void operator()(const ColumnVector<T>* source) {
        auto* casted = dynamic_cast<ColumnVector<T>*>(_res);
        bioassert(casted, "Incorrect cast for projected result column.");

        // If we are writing more rows than can fit, resize to expand
        // NOTE: Used when writing a new sorted run to memory
        if (_fromDstRow + _numRows > casted->size()) {
            casted->resize(_fromDstRow + _numRows);
        }

        // If we are writing fewer rows than we currently hold, resize to shrink
        // NOTE: Used when writing a full chunk to output
        if (_fromDstRow == 0 && _numRows < casted->size()) {
            casted->resize(_numRows);
        }

        const size_t dstSize = casted->size();

        bioassert(
            _fromSrcRow + _numRows <= _indices.size(),
            "Attempted to project rows from {} to {}, but only provided {} indices.",
            _fromSrcRow, _fromSrcRow + _numRows, _indices.size()
        );

        bioassert(dstSize >= _fromDstRow + _numRows,
                  "Attempted to write indexes [{}, {}) in Column of size {}.",
                  _fromSrcRow, _fromSrcRow + _numRows, dstSize
        );

        const auto& srcd = source->getRaw();
        const auto& indicesd = _indices.getRaw();
        auto& resd = casted->getRaw();

        for (size_t i = 0; i < _numRows; i++) {
            resd[i + _fromDstRow] = srcd[indicesd[i + _fromSrcRow]];
        }
    }

    // Any projection onto a ColumnConst is still the same columnConst
    template <typename T>
    void operator()(const ColumnConst<T>* source) {
        auto* casted = dynamic_cast<ColumnConst<T>*>(_res);
        bioassert(casted, "Incorrect cast for projected result column.");

        const T& value = source->getRaw();

        casted->set(value);
    }

private:
    Column* _res {nullptr};
    OrderByProcessor::Indices& _indices;
    size_t _numRows {0};
    size_t _fromSrcRow {0};
    size_t _fromDstRow {0};
};

/// Functor to dispatch a comparison operator on a type-erased column
class CompareInner {
public:
    CompareInner(size_t i, size_t j, int& res)
        : _i(i),
          _j(j),
          _res(res)
    {
    }

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(const ColumnVector<T>* col) {
        _res = compare(col->operator[](_i), col->operator[](_j));
    }

    // Any two positions in a ColumnConst are equal
    template <typename T>
    void operator()(const ColumnConst<T>*) { _res = 0; }
private:
    size_t _i {0};
    size_t _j {0};
    int& _res;
};

using Compare = ColumnSingleDispatcher<OrderedTypes::Allowed, CompareInner, OrderedTypes::Excluded>;

/// Helper function to sort @param indices according to two adjacent sorted runs
void mergeAdjacent(OrderByProcessor::Indices& indices,
                   const Dataframe* srcDf,
                   const OrderByProcessor::OrderByKeys& keys,
                   const OrderByProcessor::SortedRun& run1,
                   const OrderByProcessor::SortedRun& run2) {
    bioassert(run1._start == 0, "First run did not start from 0.");
    bioassert(run1._start + run1._size == run2._start,
              "Second run did not start from the end of the first.");

    // Do one pass through all Ordered keys to ensure columns are valid
    for (const auto& [tag, _] : keys) {
        const NamedColumn* ncol = srcDf->getColumn(tag);
        bioassert(ncol,
                  "Attempted to merge adjacent runs, but could not find Column {}.",
                  tag.getValue());

        bioassert(ncol->getColumn(),
                  "Attempted to merge adjacent runs, but Column {} was invalid.",
                  tag.getValue());
    }

    auto rowCmp = [&](size_t i, size_t j) {
        int comparisonResult = 0;
        CompareInner cmp(i, j, comparisonResult);

        for (const auto& [tag, asc] : keys) {
            // Validity of columns checked above
            Column* col = srcDf->getColumn(tag)->getColumn();

            Compare::dispatch(col, cmp);

            // Values are not equal; return according to direction of ordering
            if (comparisonResult != 0) {
                return asc ? comparisonResult < 0 : comparisonResult > 0;
            }
            // Values are equal, check next column
        }
        // All rows equal
        return false;
    };

    auto run1Start = std::begin(indices) + run1._start;
    auto run1End = run1Start + run1._size;

    auto run2Start = std::begin(indices) + run2._start;
    auto run2End = run2Start + run2._size;

    std::inplace_merge(run1Start, run1End, run2End, rowCmp);
}

using NarrowRanges = ColumnSingleDispatcher<OrderedTypes::Allowed,
                                            NarrowTieRanges,
                                            OrderedTypes::Excluded>;

using SubrangeSort = ColumnSingleDispatcher<OrderedTypes::Allowed,
                                            OrderColumnSubrange,
                                            OrderedTypes::Excluded>;

using Sort = ColumnSingleDispatcher<OrderedTypes::Allowed,
                                    OrderColumn,
                                    OrderedTypes::Excluded>;

using Projection = ColumnSingleDispatcher<OrderedTypes::Allowed,
                                          ProjectOrder,
                                          OrderedTypes::Excluded>;
}

OrderByProcessor::OrderByProcessor()
{
}

OrderByProcessor::~OrderByProcessor() {
}

std::string OrderByProcessor::describe() const {
    return fmt::format("OrderByProcessor@={}", fmt::ptr(this));
}

OrderByProcessor* OrderByProcessor::create(PipelineV2* pipeline,
                                           std::span<const OrderByKey> keys) {
    OrderByProcessor* proc = new OrderByProcessor();

    {
        PipelineInputPort* inputPort = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(inputPort);
        proc->addInput(inputPort);
        // Needs data in @ref State::SORT_INCOMING, but not in any others. Initially set
        // to true, set to false in @ref OrderByProcessor::execute
        inputPort->setNeedsData(true);
    }

    {
        PipelineOutputPort* outputPort = PipelineOutputPort::create(pipeline, proc);
        proc->_output.setPort(outputPort);
        proc->addOutput(outputPort);
    }

    {
        proc->_orderedKeys.reserve(keys.size());
        proc->_orderedKeys.assign(begin(keys), end(keys));
    }

    proc->postCreate(pipeline);
    return proc;
}

template <std::ranges::random_access_range Rg>
void OrderByProcessor::addTieRanges(TieRanges& tieRanges, const Rg& rg, size_t start) {
    // Find the first instance of a duplciated entry in the column
    auto startIt = std::ranges::adjacent_find(rg);
    const auto endSentinel = std::end(rg);

    while (startIt != endSentinel) {
        // Find all [start, end) ranges of duplicated entries in column
        auto endIt = startIt;
        while (endIt != endSentinel && *endIt == *startIt) {
            ++endIt;
        }
        const size_t startIdx = std::distance(std::begin(rg), startIt) + start;
        const size_t size = std::distance(startIt, endIt);
        tieRanges.emplace_back(startIdx, size);
        startIt = std::adjacent_find(endIt, std::end(rg));
    }
}

void OrderByProcessor::project(const Column* src, Column* dst, size_t numRows,
                               size_t fromSrcRow, size_t fromDstRow) {
    ProjectOrder project(dst, *_indices, numRows, fromSrcRow, fromDstRow);

    Projection::dispatch(src, project);
}

void OrderByProcessor::subsort() {
    const Dataframe* input = _input.getDataframe();
    if (input->getLogicalRowCount() == 0) {
        return;
    }

    const auto getOrderedColumn = [input](const OrderByKey& key) -> Column* {
        return input->getColumn(key._col)->getColumn();
    };

    { // Ensure all columns are equal size
        const auto orderCols = _orderedKeys | rv::transform(getOrderedColumn);

        const auto sizeIt =
            std::ranges::adjacent_find(orderCols, [](const Column* a, const Column* b) {
                return a->size() != b->size();
            });

        bioassert(sizeIt == end(orderCols),
                  "Attempted to sort non-equal length columns in ORDER BY");
    }

    { // Sort the entirety of the first column
        const OrderByKey& dominantKey = _orderedKeys.front();
        Column* dominantCol = getOrderedColumn(dominantKey);
        const bool asc = dominantKey._asc;
        const size_t size = dominantCol->size();

        _indices->resize(size);
        std::iota(_indices->begin(), _indices->end(), (size_t)0);

        OrderColumn sorter(*_indices, _tieRanges, asc);

        Sort::dispatch(dominantCol, sorter);
    }

    // If the ordering is completely determined by the first key (no tied-values), then
    // nothing else to sort.
    if (_tieRanges.empty()) {
        return;
    }

    // Sort only the subspans of tied values (stored in @ref _tiedRanges) in the remaining
    // columns
    const auto& remainingKeys = _orderedKeys | rv::drop(1);
    for (const OrderByKey& key : remainingKeys) {
        // No ties: nothing left to sort
        if (_tieRanges.empty()) {
            break;
        }

        Column* column = getOrderedColumn(key);

        OrderColumnSubrange subrangeSorter(*_indices, key._asc);

        // Sort each individual range
        for (const auto& [start, size] : _tieRanges) {
            const size_t end = start + size;

            subrangeSorter.setStart(start);
            subrangeSorter.setEnd(end);

            SubrangeSort::dispatch(column, subrangeSorter);
        }

        NarrowTieRanges narrowTieRanges(*_indices, _tieRanges);

        // Shrink tie ranges
        NarrowRanges::dispatch(column, narrowTieRanges);
    }
}

void OrderByProcessor::memorise() {
    const Dataframe* curInput = _input.getDataframe();
    if (curInput->getLogicalRowCount() == 0) {
        return;
    }

    // Determine the size of the dimensions of the sorted run to memorise, and where in
    // @ref _memory it will reside
    const size_t runStart = _nextMemoryStart;
    const size_t runLength = curInput->getLogicalRowCount();
    const size_t runEnd = runStart + runLength;

    // Both input and memory have same shape; verified in @ref prepare
    const size_t numCols = _memory.size();

    const Dataframe::NamedColumns& inputCols = curInput->cols();
    const Dataframe::NamedColumns& memoryCols = _memory.cols();

    for (size_t col = 0; col < numCols; col++) {
        const Column* inputCol = inputCols.at(col)->getColumn();
        Column* memoryCol = memoryCols.at(col)->getColumn();
        project(inputCol, memoryCol, runLength, 0, runStart);
    }

    // Log this run
    _sortedRuns.emplace_back(runStart, runLength);
    // Ensure the next sorted run starts after this one
    _nextMemoryStart = runEnd;
}

void OrderByProcessor::merge() {
    if (_sortedRuns.empty()) {
        return;
    }

    const size_t memRowCount = _memory.getLogicalRowCount();

    if (memRowCount == 0) {
        return;
    }

    // Reset @ref _indices, we now need to determine the correct order of all rows from
    // each sorted run in memory
    _indices->resize(memRowCount);
    std::iota(_indices->begin(), _indices->end(), (size_t)0);

    SortedRun mergedRun = _sortedRuns.front();

    for (const SortedRun& run : _sortedRuns | rv::drop(1)) {
        const size_t thisRunsSize = run._size;

        mergeAdjacent(*_indices, &_memory, _orderedKeys, mergedRun, run);

        mergedRun._size += thisRunsSize;
    }
}

void OrderByProcessor::prepare(ExecutionContext* ctxt) {
    bioassert(_indices, "Null indices on prepare of OrderByProcessor.");
    bioassert(_memory.hasSameShape(_output.getDataframe()),
              "Memory and output mismatch in OrderByProcessor.");

    _ctxt = ctxt;
    markAsPrepared();
}

void OrderByProcessor::reset() {
    markAsReset();
}

void OrderByProcessor::execute() {
    bioassert(_indices, "Null indices in OrderByProcessor.");

    PipelineInputPort* inputPort = _input.getPort();
    PipelineOutputPort* outputPort = _output.getPort();

    if (_state == State::SORT_INCOMING) {
        // Input guaranteed to have data via @ref _needsData of input
        if (inputPort->hasData()) {
            subsort();
            memorise();
            inputPort->consume();
        }

        // All input runs have been sorted and memorised
        if (inputPort->isClosed()) {
            _state = State::MERGE_SORTED_RUNS;
            // No longer need data to execute
            inputPort->setNeedsData(false);
        }
    } else if (_state == State::MERGE_SORTED_RUNS) {
        merge();
        // @ref _indices now contains total order of all rows from memory

        _state = State::EMIT_FROM_MEMORY;

         // Reuse this member as a pointer to how far through memory we have emitted
        _nextMemoryStart = 0;
    } else if (_state == State::EMIT_FROM_MEMORY) {
         const size_t memoryRowCount = _memory.getLogicalRowCount();
         const size_t remainingToWrite = memoryRowCount - _nextMemoryStart;

         if (remainingToWrite == 0) {

             // Emit an empty chunk if and only if the memory was empty, allowing next
             // processor to proceed.
             if (memoryRowCount == 0) {
                 outputPort->writeData();
             }

             finish();
             return;
         }

         const size_t rowsToWrite = std::min(remainingToWrite, _ctxt->getChunkSize());

         const Dataframe* outDf = _output.getDataframe();

         for (size_t i = 0; i < outDf->size(); i++) {
             const Column* memoryCol = _memory.cols()[i]->getColumn();
             Column* outCol = outDf->cols()[i]->getColumn();

             static constexpr size_t OUTPUT_START = 0;
             project(memoryCol, outCol, rowsToWrite, _nextMemoryStart, OUTPUT_START);
         }

         outputPort->writeData();

         _nextMemoryStart += rowsToWrite;

         if (_nextMemoryStart == memoryRowCount) {
             finish();
         }
    } else {
        throw FatalException("OrderByProcessor was in an invalid state.");
    }
}

