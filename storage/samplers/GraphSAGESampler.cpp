#include "GraphSAGESampler.h"
#include "columns/ColumnIDs.h"
#include "columns/ColumnOptVector.h"
#include "iterators/ChunkConfig.h"
#include "iterators/NeighbourhoodSampleIterator.h"
#include <algorithm>

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

GraphSAGESampler::GraphSAGESampler(const GraphView* view, const ColumnNodeIDs* seeds,
                                   Fanouts fanouts)
    : _view(view),
      _seeds(seeds),
      _fanouts(fanouts) {
}

void GraphSAGESampler::sample() {
    // dst_nodes for step 1 is precisely the seeds
    colAssign(_seeds, _dstNodes1);
    _requiredLength = std::max(_requiredLength, _seeds->size());

    ColumnNodeIDs tmpSrc;
    ColumnNodeIDs tmpTgt;
    ColumnNodeIDs tmpSeeds;

    const auto sample = [&](size_t step,
                            const ColumnOptVector<NodeID>* seeds,
                            ColumnOptVector<NodeID>* srcs,
                            ColumnOptVector<NodeID>* tgts) -> void {
        bioassert(step < _fanouts.size(), "Invalid step");
        const size_t sampleSize = _fanouts[step];
        colAssign(seeds, &tmpSeeds);
        NeighbourhoodSampleChunkWriter writer(*_view, &tmpSeeds, sampleSize);

        // TODO: avoid intermediate copy and pass opt vec directly
        tmpSrc.clear(), tmpTgt.clear();
        writer.setOutputColumns(&tmpSrc, nullptr, nullptr, &tmpTgt);
        writer.fill(ChunkConfig::CHUNK_SIZE);

        colAssign(&tmpSrc, srcs);
        colAssign(&tmpTgt, tgts);

        bioassert(srcs->size() == tgts->size(), "Invalid sample");
        _requiredLength = std::max(_requiredLength, srcs->size());
    };

    // hop1
    sample(0, _dstNodes1, _srcs1, _tgts1);
    deduplicate(_tgts1, _dstNodes2);

    // hop2
    sample(1, _dstNodes2, _srcs2, _tgts2);
    deduplicate(_tgts2, _dstNodes3);

    // hop3
    sample(2, _dstNodes3, _srcs3, _tgts3);
}
