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

GraphSAGESampler::GraphSAGESampler(GraphView view)
    : _view(view)
{
}

void GraphSAGESampler::setHopData(size_t idx, NodeCol* srcs, NodeCol* tgts, NodeCol* dst, size_t fanout) {
    bioassert(idx < hops, "Tried to set OOB hop data");
    HopData& hopData = _sampleData[idx];

    hopData._fanout = fanout;
    hopData._srcs = srcs;
    hopData._dstNodes = dst;
    hopData._tgts = tgts;
}

void GraphSAGESampler::sample(const ColumnNodeIDs* seeds) {
    static_assert(hops >= 1);

    _requiredLength = std::max(_requiredLength, seeds->size());

    // first hops dst_nodes are the query seeds
    colAssign(seeds, _sampleData[0]._dstNodes);
    deduplicate(_sampleData[0]._dstNodes, _sampleData[0]._dstNodes);

    while (_currentHop < hops) {
        sampleHop();
    }

    // padd all columns with nulls to ensure square dataframe
    for (HopData& data : _sampleData) {
        data.resize(_requiredLength);
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

    NeighbourhoodSampleChunkWriter writer(_view, &tmpSeeds, sampleSize);

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
