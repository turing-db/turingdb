#pragma once

#include <limits>
#include <memory>
#include <stddef.h>
#include <unordered_set>

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

    explicit GraphSAGESampler(GraphView view, size_t seed = NOSEED);

    void setHopData(size_t idx, NodeCol* srcs, NodeCol* tgts, NodeCol* dst, size_t fanout);

    void seed(const ColumnNodeIDs* seeds);

    void sample(size_t maxRows);

    void reset();

    bool finished() const;

private:
    struct HopData;
    using Samples = std::array<HopData, hops>;
    // Cypher parser prevents SIZE_MAX from being entered, meaning it is a valid sentinel
    // which is always distinguishable from a user-specified seed

    // TODO: check whether dst_nodes should include srcs of previous
    // i.e. unique(seeds_k ∪ tgts_k)
    struct HopColumns {
        NodeCol* _dstNodes {nullptr}; // nodes to generate embeddings for this hop (seeds)
        NodeCol* _srcs {nullptr};     // src nodes of edges for this hop
        NodeCol* _tgts {nullptr};     // tgt nodes of edges for this hop
    };

    struct HopData final : HopColumns {
        size_t _fanout {0};           // neighbourhood sample size for this hop

        // The frontier is appended to as the previous hop emits, and the writer reads it
        // by index, so it picks up nodes that arrive after it reached the end
        ColumnNodeIDs _frontier;
        std::unordered_set<uint64_t> _seen;
        std::unique_ptr<NullableNeighbourhoodSampleWriter> _writer;

        // A frontier node is one dst_nodes row but up to _fanout edge rows, so the two
        // fill at different rates and dst_nodes needs a cursor of its own
        size_t _emitted {0};

        bool finished() const;

        void reset();

        void resize(size_t size);
        void clear();
        template <typename F, typename... Args>
        void apply(const F& func, Args&&... args);
    };

    GraphView _view;

    Samples _sampleData {};

    NodeCol _srcsScratch; // used if srcs not yielded

    size_t _seed {NOSEED};

    bool _seeded {false};

    void pushNode(HopData& data, NodeID node);
    void pushFrontier(size_t hop, const ColumnNodeIDs* nodes);
    void pushFrontier(size_t hop, const NodeCol* nodes);
    size_t emitFrontier(size_t hop, size_t maxRows);
    size_t expandHop(size_t hop, size_t maxRows);
};

}
