#include "GraphSAGESampler.h"

#include <algorithm>
#include <optional>
#include <stddef.h>

#include "iterators/NeighbourhoodSampleIterator.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"

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

}

GraphSAGESampler::GraphSAGESampler(GraphView view, size_t seed)
    : _view(view),
    _seed(seed)
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

void GraphSAGESampler::setHopData(size_t idx, NodeCol* srcs, NodeCol* tgts, NodeCol* dst, size_t fanout) {
    bioassert(idx < hops, "Tried to set OOB hop data");
    HopData& hopData = _sampleData[idx];

    hopData._fanout = fanout;
    hopData._srcs = srcs;
    hopData._dstNodes = dst;
    hopData._tgts = tgts;
}

void GraphSAGESampler::reset() {
    for (HopData& data : _sampleData) {
        data.clear();
        data.reset();
    }
}

bool GraphSAGESampler::finished() const {
    return _seeded && std::ranges::all_of(_sampleData, &HopData::finished);
}

void GraphSAGESampler::seed(const ColumnNodeIDs* seeds) {
    static_assert(hops >= 1);

    pushFrontier(0, seeds);

    _seeded = true;
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

void GraphSAGESampler::pushFrontier(size_t hop, const NodeCol* nodes) {
    bioassert(hop < hops, "Tried to seed an OOB hop");
    HopData& data = _sampleData[hop];

    for (const std::optional<NodeID>& node : *nodes) {
        if (!node.has_value()) {
            continue;
        }

        pushNode(data, *node);
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

    if (data._fanout == 0 || data._frontier.empty()) {
        return 0;
    }

    if (!data._writer) {
        const bool haveSeed = _seed != NOSEED;
        data._writer = haveSeed
            ? std::make_unique<NullableNeighbourhoodSampleWriter>(_view, &data._frontier, data._fanout, _seed)
            : std::make_unique<NullableNeighbourhoodSampleWriter>(_view, &data._frontier, data._fanout);
    }

    std::unique_ptr<NullableNeighbourhoodSampleWriter>& writer = data._writer;

    if (writer->isDone()) {
        return 0;
    }

    NodeCol* srcs = data._srcs ? data._srcs : &_srcsScratch;

    writer->setOutputColumns(srcs, nullptr, nullptr, data._tgts);

    writer->fill(maxRows);

    if (data._tgts) {
        bioassert(srcs->size() == data._tgts->size(), "Mismatched srcs, tgts");
    }

    if (hop + 1 < hops) {
        pushFrontier(hop + 1, srcs);
    }

    return srcs->size();
}

void GraphSAGESampler::sample(size_t maxRows) {
    bioassert(_seeded, "Attempted to sample without seeding");
    bioassert(maxRows > 0, "maxRows was zero");

    for (const HopData& data : _sampleData) {
        bioassert(maxRows >= data._fanout, "Row budget is narrower than a hop's fanout");
    }

    for (HopData& data : _sampleData) {
        data.clear();
    }

    size_t requiredRowCount = 0; // ensure all columns are the same size
    for (size_t hop = 0; hop < hops; hop++) {
        const size_t frontierSize = emitFrontier(hop, maxRows);
        const size_t hopSize = expandHop(hop, maxRows);

        requiredRowCount = std::max({frontierSize, hopSize, requiredRowCount});
    }

    // null-extend columns to ensure rectangularity
    for (HopData& data : _sampleData) {
        data.resize(requiredRowCount);
    }
}

template void GraphSAGESampler::HopData::apply<decltype(resizeImpl), size_t>(const decltype(resizeImpl)&, size_t&&);
template void GraphSAGESampler::HopData::apply<decltype(clearImpl)>(const decltype(clearImpl)&);
