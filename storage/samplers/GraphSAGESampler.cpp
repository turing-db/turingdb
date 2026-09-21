#include "GraphSAGESampler.h"

#include <algorithm>
#include <stddef.h>

#include "iterators/ChunkConfig.h"
#include "iterators/NeighbourhoodSampleIterator.h"

#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"


using namespace db;

static void colAssign(const ColumnNodeIDs* src, ColumnOptVector<NodeID>* dst) {
    auto& raw = dst->getRaw();
    raw.assign(src->begin(), src->end());
}

static void colAssign(const ColumnOptVector<NodeID>* src, ColumnNodeIDs* dst) {
    dst->clear();
    for (std::optional<NodeID> n : *src) {
        if (n.has_value()) {
            dst->push_back(*n);
        }
    }
}

static void deduplicate(const ColumnOptVector<NodeID>* src, ColumnOptVector<NodeID>* dst) {
    dst->assign(src);
    auto& raw = dst->getRaw();
    std::ranges::sort(raw);
    auto [newEnd, oldEnd] = std::ranges::unique(raw);
    raw.erase(newEnd, oldEnd);
}

namespace {
const auto resizeImpl = [](auto* col, size_t size) -> void { col->resize(size); };
const auto clearImpl = [](auto* col) -> void { col->clear(); };
}

GraphSAGESampler::GraphSAGESampler(GraphView view, size_t seed)
    : _view(view),
    _seed(seed)
{
}

template <typename F, typename... Args>
void GraphSAGESampler::HopData::apply(const F& func, Args&&... args) {
    func(_dstNodes, std::forward<Args>(args)...);
    func(_srcs, std::forward<Args>(args)...);
    func(_tgts, std::forward<Args>(args)...);

    constexpr size_t numCols = (sizeof(HopData) - sizeof(_fanout)) / sizeof(NodeCol*);
    static_assert(numCols == 3, "Member added, update apply.");
}

void GraphSAGESampler::HopData::resize(size_t size) {
    apply(resizeImpl, size);
}

void GraphSAGESampler::HopData::clear() {
    apply(clearImpl);
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
    for (HopData& d : _sampleData) {
        d.clear();
    }
    _requiredLength = 0;
    _currentHop = 0;
    _seeded = false;
    _finished = false;
}

void GraphSAGESampler::seed(const ColumnNodeIDs* seeds) {
    _requiredLength = std::max(_requiredLength, seeds->size());

    // first hop's dst_nodes are the query seeds
    colAssign(seeds, _sampleData[0]._dstNodes);
    deduplicate(_sampleData[0]._dstNodes, _sampleData[0]._dstNodes);

    _seeded = true;
}

void GraphSAGESampler::sample() {
    static_assert(hops >= 1);

    bioassert(_seeded, "Attempted to sample without seeding");

    while (_currentHop < hops) {
        sampleHop();
    }

    // pad all columns with nulls to ensure all columns are square
    for (HopData& data : _sampleData) {
        data.resize(_requiredLength);
    }

    if (_currentHop == hops) {
        _finished = true;
    }
}

// XXX: TODO: Chunking behaviour
void GraphSAGESampler::sampleHop() {
    bioassert(_currentHop < hops, "Tried to sample with OOB hop number");

    HopData& thisHop = _sampleData[_currentHop];
    NodeCol* seeds = thisHop._dstNodes;

    ColumnNodeIDs tmpSeeds;
    colAssign(seeds, &tmpSeeds);

    const size_t sampleSize = thisHop._fanout;

    const bool haveSeed = _seed != NOSEED;
    NeighbourhoodSampleChunkWriter writer =
        haveSeed ? NeighbourhoodSampleChunkWriter(_view, &tmpSeeds, sampleSize, _seed)
                 : NeighbourhoodSampleChunkWriter(_view, &tmpSeeds, sampleSize);

    ColumnNodeIDs tmpSrcs;
    ColumnNodeIDs tmpTgts;
    writer.setOutputColumns(&tmpSrcs, nullptr, nullptr, &tmpTgts);
    // XXX: Check for overflowing a chunk, maybe loop untilDone
    writer.fill(ChunkConfig::CHUNK_SIZE);

    NodeCol* thisSrcs = thisHop._srcs;
    NodeCol* thisTgts = thisHop._tgts;

    colAssign(&tmpSrcs, thisSrcs);
    colAssign(&tmpTgts, thisTgts);

    bioassert(thisSrcs->size() == thisTgts->size(), "Mismatched srcs, tgts");
    _requiredLength = std::max(_requiredLength, thisSrcs->size());

    if (_currentHop == hops - 1) {
        _currentHop++;
        return;
    }

    HopData& nextHop = _sampleData[_currentHop + 1];
    NodeCol* nextDst = nextHop._dstNodes;
    deduplicate(thisHop._tgts, nextDst); // seed the next hop with the targets of current

    _currentHop++;
}

template void GraphSAGESampler::HopData::apply<decltype(resizeImpl), size_t>(const decltype(resizeImpl)&, size_t&&);
template void GraphSAGESampler::HopData::apply<decltype(clearImpl)>(const decltype(clearImpl)&);
