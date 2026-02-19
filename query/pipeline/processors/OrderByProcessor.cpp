#include "OrderByProcessor.h"

#include <algorithm>
#include <concepts>
#include <numeric>

#include <range/v3/action/sort.hpp>
#include <range/v3/view/drop.hpp>
#include <range/v3/view/subrange.hpp>
#include <range/v3/view/transform.hpp>

#include <spdlog/fmt/bundled/format.h>

#include "PipelinePort.h"
#include "columns/AllowedKinds.h"
#include "columns/Column.h"
#include "columns/ColumnOperatorDispatcher.h"
#include "columns/ColumnOperators.h"
#include "dataframe/Dataframe.h"

#include "BioAssert.h"
#include "FatalException.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

struct OrderColumn {
    OrderByProcessor::Indices& _indices;
    OrderByProcessor::TieRanges& _ranges;
    bool _ascending {true};

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(const ColumnVector<T>* typed) {
        const std::vector<T>& data = typed->getRaw();

        // Sort the indices by indexing the column
        if (_ascending) {
            rg::sort(_indices, [&](size_t i, size_t j) { return data[i] < data[j]; });
        } else {
            rg::sort(_indices, [&](size_t i, size_t j) { return data[i] > data[j]; });
        }

        // Get a view of the column with the sorted indices
        auto reordered = _indices
                         | rv::transform([&](std::size_t i) -> auto& { return data[i]; });

        OrderByProcessor::addTieRanges(_ranges, reordered);
    }
};

struct OrderColumnSubrange {
    OrderByProcessor::Indices& _indices;
    size_t _subrangeStart {0};
    size_t _subrangeEnd {0};
    bool _asc {true};

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
};

struct NarrowTieRanges {
    OrderByProcessor::Indices& _indices;
    OrderByProcessor::TieRanges& _ranges;

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(const ColumnVector<T>* typed) {
        const std::vector<T>& data = typed->getRaw();

        auto reordered = _indices
                         | rv::transform([&](std::size_t i) -> auto& { return data[i]; });

        // Temporary vector which will contain the new tie-ranges
        OrderByProcessor::TieRanges temp;

        for (const auto& [start, size] : _ranges) {
            const size_t end = start + size;

            const auto beginIt = std::begin(reordered) + start;
            const auto endIt = std::begin(reordered) + end;
            const auto tiedRange = rg::subrange(beginIt, endIt);

            OrderByProcessor::addTieRanges(temp, tiedRange, start);
        }

        _ranges.swap(temp);
    }
};

struct ProjectOrder {
    Column* _res {nullptr};
    ColumnVector<size_t>* _indices {nullptr};

    template <typename T>
    void operator()(const ColumnVector<T>* source) {
        auto* casted = dynamic_cast<ColumnVector<T>*>(_res);
        bioassert(casted, "Incorrect cast for projected result column.");

        ColumnOperators::copyTransformedChunk(_indices, source, casted);
    }
};

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
                                           std::span<OrderByKey> keys) {
    OrderByProcessor* proc = new OrderByProcessor;

    {
        PipelineInputPort* inputPort = PipelineInputPort::create(pipeline, proc);
        proc->_input.setPort(inputPort);
        proc->addInput(inputPort);
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

void OrderByProcessor::prepare(ExecutionContext* ctxt) {
    markAsPrepared();
}

void OrderByProcessor::reset() {
    markAsReset();
}

template <std::ranges::random_access_range Rg>
void OrderByProcessor::addTieRanges(TieRanges& tieRanges, const Rg& rg, size_t start) {
    // Find the first instance of a duplciated entry in the column
    auto startIt = std::ranges::adjacent_find(rg);

    while (startIt != std::end(rg)) {
        // Find the interval [start, end) of duplicated entries in column
        auto endIt = startIt;
        while (endIt != std::end(rg) && *endIt == *startIt) {
            ++endIt;
        }
        const size_t startIdx = std::distance(std::begin(rg), startIt) + start;
        const size_t size = std::distance(startIt, endIt);
        tieRanges.emplace_back(startIdx, size);
        startIt = std::adjacent_find(endIt, std::end(rg));
    }
}

void OrderByProcessor::project() {
    const Dataframe* inputDf = _input.getDataframe();
    const auto& inputCols = inputDf->cols();
    const Dataframe* outputDf = _output.getDataframe();

    // Project indices onto the output
    for (const NamedColumn* ncol : inputCols) {
        const ColumnTag inTag = ncol->getTag();

        NamedColumn* outNcol = outputDf->getColumn(inTag);

        if (!outNcol) {
            throw FatalException(fmt::format(
                "Failed to get output column for sorting column {}.", inTag.getValue()));
        }

        const Column* inCol = ncol->getColumn();
        Column* outCol = outNcol->getColumn();

        using Types = OrderedTypes;
        ProjectOrder project {._res = outCol, ._indices = _indices};
        using Projection =
            ColumnSingleDispatcher<Types::Allowed, ProjectOrder, Types::Excluded>;

        Projection::dispatch(inCol, project);
    }
}

void OrderByProcessor::subsort() {
    // spdlog::info("OrderBy::subsort");
    // Ensure all columns are equal size
    const auto sizeIt = std::ranges::adjacent_find(_orderedKeys,
                               [](const OrderByKey& a, const OrderByKey& b) {
                                   return a._col->size() != b._col->size();
                               });
    bioassert(sizeIt == end(_orderedKeys),
              "Attempted to sort non-equal length columns in ORDER BY");

    { // Sort the entirety of the first column
        const OrderByKey& dominantKey = _orderedKeys.front();
        Column* dominantCol = dominantKey._col;
        const bool asc = dominantKey._asc;
        const size_t size = dominantCol->size();

        _indices->resize(size);
        std::ranges::iota(*_indices, 0);

        OrderColumn sorter {
            ._indices = *_indices, ._ranges = _tieRanges, ._ascending = asc};

        using Sort = ColumnSingleDispatcher<OrderedTypes::Allowed, OrderColumn,
                                            OrderedTypes::Excluded>;

        Sort::dispatch(dominantCol, sorter);
    }

    // If the ordering is completely determined by the first key (no tied-values), then
    // nothing else to sort.
    if (_tieRanges.empty()) {
        project();
        return;
    }

    // Sort only the subspans of tied values (stored in @ref _tiedRanges) in the remaining
    // columns
    const auto& remainingKeys = _orderedKeys | rv::drop(1);
    for (auto& [column, asc] : remainingKeys) {
        // No ties: nothing left to sort
        if (_tieRanges.empty()) {
            break;
        }

        OrderColumnSubrange subrangeSorter {._indices = *_indices, ._asc = asc};

        // Sort each individual range
        for (const auto& [start, size] : _tieRanges) {
            const size_t end = start + size;

            subrangeSorter._subrangeStart = start;
            subrangeSorter._subrangeEnd = end;

            using SubrangeSort = ColumnSingleDispatcher<OrderedTypes::Allowed,
                                                        OrderColumnSubrange,
                                                        OrderedTypes::Excluded>;

            SubrangeSort::dispatch(column, subrangeSorter);
        }

        NarrowTieRanges narrowTieRanges {._indices = *_indices, ._ranges = _tieRanges};

        using NarrowRanges = ColumnSingleDispatcher<OrderedTypes::Allowed,
                                                    NarrowTieRanges,
                                                    OrderedTypes::Excluded>;

        // Shrink tie ranges
        NarrowRanges::dispatch(column, narrowTieRanges);
    }

    project();
}

// TODO: Handle ColumnConst as order key
void OrderByProcessor::execute() {
    bioassert(_indices, "Null indices in OrderByProcessor.");

    if (_state == State::SORT_INCOMING) {
        subsort();
    }

    // Always finish in one cycle
    _input.getPort()->consume();
    _output.getPort()->writeData();

    finish();
    return;

    if (_state == State::MERGE_SORTED_RUNS) {
        return;
    }
}

