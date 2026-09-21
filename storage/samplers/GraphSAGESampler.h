#pragma once

#include <limits>
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
    static constexpr size_t NOSEED = std::numeric_limits<size_t>::max();

    GraphSAGESampler(GraphView view, size_t seed = NOSEED);

    void setHopData(size_t idx, NodeCol* srcs, NodeCol* tgts, NodeCol* dst, size_t fanout);

    void seed(const ColumnNodeIDs* seeds);

    void sample();

    void reset();

    bool finished() const { return _finished; }

private:
    struct HopData;
    using Samples = std::array<HopData, hops>;
    // Cypher parser prevents SIZE_MAX from being entered, meaning it is a valid sentinel
    // which is always distinguishable from a user-specified seed

    // TODO: check whether dst_nodes should include srcs of previous
    // i.e. unique(seeds_k ∪ tgts_k)
    struct HopData {
        NodeCol* _dstNodes {nullptr}; // nodes to generate embeddings for this hop (seeds)
        NodeCol* _srcs {nullptr};     // src nodes of edges for this hop
        NodeCol* _tgts {nullptr};     // tgt nodes of edges for this hop
        size_t _fanout {0};           // neighbourhood sample size for this hop
        void resize(size_t size);
        void clear();
        template <typename F, typename... Args>
        void apply(const F& func, Args&&... args);
    };

    GraphView _view;

    Samples _sampleData {};

    size_t _requiredLength {0};
    size_t _currentHop {0};

    size_t _seed {NOSEED};

    bool _seeded {false};

    bool _finished {false};

    void sampleHop();
};

}
