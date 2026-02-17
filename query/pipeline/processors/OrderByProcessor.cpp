#include "OrderByProcessor.h"

#include <algorithm>
#include <concepts>
#include <numeric>

#include <range/v3/action/sort.hpp>
#include <range/v3/view/transform.hpp>

#include <spdlog/fmt/bundled/format.h>

#include "PipelinePort.h"
#include "columns/Column.h"

#include "BioAssert.h"

using namespace db;

namespace rg = ranges;
namespace rv = rg::views;

namespace {

template <bool Ascending>
struct OrderColumn {
    OrderByProcessor::Indices& _indices;
    OrderByProcessor::TieRanges& _ranges;

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(ColumnVector<T>& typed) {
        std::vector<T>& data = typed.getRaw();

        // Sort the indices by indexing the column
        if constexpr (Ascending) {
            rg::sort(_indices, [&](size_t i, size_t j) { return data[i] < data[j]; });
        } else {
            rg::sort(_indices, [&](size_t i, size_t j) { return data[i] > data[j]; });
        }

        // Get a view of the column with the sorted indices
        auto reordered =
            _indices | rv::transform([&](std::size_t i) -> auto& { return data[i]; });

        OrderByProcessor::addTieRanges(_ranges, data);
    }
};

template <bool Ascending>
struct OrderColumnSubrange {
    OrderByProcessor::Indices& _indices;
    size_t subrangeStart {0};
    size_t subrangeEnd {0};

    template <typename T>
        requires std::totally_ordered<T>
    void operator()(ColumnVector<T>& typed) {
        std::vector<T>& data = typed.getRaw();

        const auto beginIt = begin(_indices) + subrangeStart;
        const auto endIt = begin(_indices) + subrangeEnd;

        if constexpr (Ascending) {
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
    void operator()(ColumnVector<T>& typed) {
        std::vector<T>& data = typed->getRaw();

        auto reordered =
            _indices | rv::transform([&](std::size_t i) -> auto& { return data[i]; });

        // Temporary vector which will contain the new tie-ranges
        OrderByProcessor::TieRanges temp;

        for (const auto& [start, size] : _ranges) {
            const size_t end = start + size;

            const auto beginIt = begin(reordered) + start;
            const auto endIt = begin(reordered) + end;

            OrderByProcessor::addTieRanges(temp, beginIt, endIt, start);
        }

        _ranges.swap(temp);
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

    while (startIt != end(rg)) {
        // Find the interval [start, end) of duplicated entries in column
        auto endIt = startIt;
        while (endIt != end(rg) && *endIt == *startIt) {
            ++endIt;
        }
        const size_t startIdx = std::distance(begin(rg), startIt) + start;
        const size_t size = std::distance(startIt, endIt);
        tieRanges.emplace_back(startIdx, size);
        startIt = std::adjacent_find(endIt, end(rg));
    }
}

void OrderByProcessor::subsort() {
    // Ensure all columns are equal size
    const auto sizeIt = std::ranges::adjacent_find(_orderedKeys,
                               [](const OrderByKey& a, const OrderByKey& b) {
                                   return a._col->size() != b._col->size();
                               });
    bioassert(sizeIt == end(_orderedKeys),
              "Attempted to sort non-equal length columns in ORDER BY");

    Column* dominantCol = _orderedKeys.front()._col;
    const size_t size = dominantCol->size();

    _indices.resize(size);
    std::ranges::iota(_indices, 0);
}

// TODO:
// - Handle ColumnConst as order key
void OrderByProcessor::execute() {
    if (_state == State::SORT_INCOMING) {
        return;
    }

    if (_state == State::MERGE_SORTED_RUNS) {
        return;
    }
}

