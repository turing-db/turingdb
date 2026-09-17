#pragma once

#include <stddef.h>

#include "columns/ColumnIDs.h"
#include "iterators/NeighbourhoodSampleIterator.h"

#include "columns/ColumnOptVector.h"

namespace db {

class GraphSAGESampler {
public:
    using NodeCol = ColumnOptVector<NodeID>;
    constexpr static size_t hops = 3;
    using Fanouts = std::array<size_t, hops>;

    explicit GraphSAGESampler(const GraphView* view);

    void setHopData(size_t idx, NodeCol* srcs, NodeCol* tgts, NodeCol* dst, size_t fanout);

    void sample(const ColumnNodeIDs* seeds);

private:
    struct HopData;
    using Samples = std::array<HopData, hops>;

    // TODO: check whether dst_nodes should include srcs of previous
    // i.e. unique(seeds_k ∪ tgts_k)
    struct HopData {
        NodeCol* _srcs {nullptr};     // src nodes of edges for this hop
        NodeCol* _tgts {nullptr};     // tgt nodes of edges for this hop
        NodeCol* _dstNodes {nullptr}; // nodes to generate embeddings for this hop (seeds)
        size_t _fanout {0};           // neighbourhood sample size for this hop
    };

    const GraphView* _view {nullptr};

    Samples _sampleData {};

    size_t _requiredLength {0};
    size_t _currentHop {0};

    void sampleHop();
};

}
