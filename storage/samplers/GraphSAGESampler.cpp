#include "GraphSAGESampler.h"

#include <algorithm>
#include <optional>
#include <stddef.h>

#include "iterators/NeighbourhoodSampleIterator.h"

#include "columns/ColumnIDs.h"

using namespace db;

namespace {

const auto resizeImpl = [](auto* col, size_t size) -> void {
    if (!col) {
        return;
    }
    col->resize(size);
};

const auto clearImpl = [](auto* col) -> void {
    if (!col) {
        return;
    }
    col->clear();
};

void appendNodes(GraphSAGESampler::NodeCol* column, const ColumnNodeIDs& nodes) {
    if (!column) {
        return;
    }

    auto& raw = column->getRaw();
    raw.insert(raw.end(), nodes.cbegin(), nodes.cend());
}

}

GraphSAGESampler::GraphSAGESampler(GraphView view)
    : _view(view)
{
}

template <typename F, typename... Args>
void GraphSAGESampler::HopData::apply(const F& func, Args&&... args) {
    if (_dstNodes) {
        func(_dstNodes, std::forward<Args>(args)...);
    }

    if (_srcs) {
        func(_srcs, std::forward<Args>(args)...);
    }

    if (_tgts) {
        func(_tgts, std::forward<Args>(args)...);
    }

    constexpr size_t numCols = sizeof(HopColumns) / sizeof(NodeCol*);
    static_assert(numCols == 3, "Member added, update apply.");
}

size_t GraphSAGESampler::HopData::size() const {
    for (const NodeCol* column : {_dstNodes, _srcs, _tgts}) {
        if (column) {
            return column->size();
        }
    }

    return 0;
}

void GraphSAGESampler::HopData::resize(size_t size) {
    apply(resizeImpl, size);
}

void GraphSAGESampler::HopData::clear() {
    apply(clearImpl);
}

bool GraphSAGESampler::HopData::finished() const {
    const bool noExpand = _frontier.empty() || _fanout == 0;
    const bool expandedAll = noExpand || (_writer && _writer->isDone());
    const bool emittedAll = _emitted == _frontier.size();

    return expandedAll && emittedAll;
}

void GraphSAGESampler::HopData::reset() {
    _frontier.clear();
    _seen.clear();
    _writer.reset();
    _emitted = 0;
}

void GraphSAGESampler::setHopColumns(size_t hop, NodeCol* srcs, NodeCol* tgts, NodeCol* dst) {
    bioassert(hop < hops, "Tried to set OOB hop data");
    HopData& hopData = _sampleData[hop];

    hopData._srcs = srcs;
    hopData._dstNodes = dst;
    hopData._tgts = tgts;
}

void GraphSAGESampler::clearColumns() {
    for (HopData& data : _sampleData) {
        data.clear();
    }
}

bool GraphSAGESampler::finished() const {
    return _started && std::ranges::all_of(_sampleData, &HopData::finished);
}

void GraphSAGESampler::start(const ColumnNodeIDs* seeds, const Fanouts& fanouts, std::optional<uint64_t> rngSeed) {
    static_assert(hops >= 1);

    _rngSeed = rngSeed;

    for (size_t hop = 0; hop < hops; hop++) {
        HopData& data = _sampleData[hop];
        data.reset();
        data._fanout = fanouts[hop];
    }

    pushFrontier(0, seeds);

    _started = true;
}

void GraphSAGESampler::pushNode(HopData& data, NodeID node) {
    const bool inserted = data._seen.insert(node.getValue()).second;
    if (!inserted) {
        return;
    }

    data._frontier.push_back(node);
}

void GraphSAGESampler::pushFrontier(size_t hop, const ColumnNodeIDs* nodes) {
    bioassert(hop < hops, "Tried to seed an OOB hop");
    HopData& data = _sampleData[hop];

    for (const NodeID node : *nodes) {
        pushNode(data, node);
    }
}

size_t GraphSAGESampler::emitFrontier(size_t hop, size_t maxRows) {
    HopData& data = _sampleData[hop];

    const size_t available = data._frontier.size() - data._emitted;
    const size_t rows = std::min(available, maxRows);
    if (rows == 0) {
        return 0;
    }

    if (data._dstNodes) {
        const ColumnNodeIDs::ConstIterator begin = data._frontier.cbegin() + data._emitted;
        auto& raw = data._dstNodes->getRaw();
        raw.insert(raw.end(), begin, begin + rows);
    }

    data._emitted += rows;

    return rows;
}

size_t GraphSAGESampler::expandHop(size_t hop, size_t maxRows) {
    HopData& data = _sampleData[hop];

    if (data._fanout == 0 || data._fanout > maxRows || data._frontier.empty()) {
        return 0;
    }

    if (!data._writer) {
        data._writer = std::make_unique<NeighbourhoodSampleWriter>(_view, &data._frontier, data._fanout, _rngSeed);
        data._writer->setOutputColumns(&_drawnSrcs, nullptr, nullptr, data._tgts ? &_drawnTgts : nullptr);
    }

    std::unique_ptr<NeighbourhoodSampleWriter>& writer = data._writer;

    if (writer->isDone()) {
        return 0;
    }

    writer->fill(maxRows);

    appendNodes(data._srcs, _drawnSrcs);
    appendNodes(data._tgts, _drawnTgts);

    if (hop + 1 < hops) {
        pushFrontier(hop + 1, &_drawnSrcs);
    }

    return _drawnSrcs.size();
}

size_t GraphSAGESampler::sample(size_t maxRows) {
    bioassert(_started, "Attempted to sample without starting");
    bioassert(maxRows > 0, "maxRows was zero");

    size_t columnRows = 0;
    for (const HopData& data : _sampleData) {
        columnRows = std::max(columnRows, data.size());
    }

    size_t requiredRowCount = 0; // ensure all columns are the same size
    for (size_t hop = 0; hop < hops; hop++) {
        const size_t frontierSize = emitFrontier(hop, maxRows);
        const size_t hopSize = expandHop(hop, maxRows);

        requiredRowCount = std::max({frontierSize, hopSize, requiredRowCount});
    }

    // null-extend columns to ensure rectangularity
    for (HopData& data : _sampleData) {
        data.resize(columnRows + requiredRowCount);
    }

    return requiredRowCount;
}

template void GraphSAGESampler::HopData::apply<decltype(resizeImpl), size_t>(const decltype(resizeImpl)&, size_t&&);
template void GraphSAGESampler::HopData::apply<decltype(clearImpl)>(const decltype(clearImpl)&);
