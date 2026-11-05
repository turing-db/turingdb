#pragma once

#include <array>
#include <memory>
#include <optional>
#include <stddef.h>
#include <stdint.h>
#include <unordered_set>

#include "columns/ColumnIDs.h"
#include "iterators/NeighbourhoodSampleIterator.h"

namespace db {

class GraphSAGESampler {
public:
    using NodeCol = ColumnNodeIDs;
    constexpr static size_t hops = 3;
    using Fanouts = std::array<size_t, hops>;

    explicit GraphSAGESampler(GraphView view);

    void setHopColumns(size_t hop, NodeCol* srcs, NodeCol* tgts, NodeCol* dst);

    // Drops what remained of the previous sample
    void start(const ColumnNodeIDs* seeds, const Fanouts& fanouts, std::optional<uint64_t> rngSeed);

    // Appends at most maxRows rows to the hop columns, which all end on the same row, and
    // returns how many. A row a column has no node for holds the invalid ID. A hop whose
    // fanout is wider than maxRows waits for a later call.
    size_t sample(size_t maxRows);

    void clearColumns();

    bool finished() const;

private:
    struct HopData;
    using Samples = std::array<HopData, hops>;

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
        std::unique_ptr<NeighbourhoodSampleWriter> _writer;

        // A frontier node is one dst_nodes row but up to _fanout edge rows, so the two
        // fill at different rates and dst_nodes needs a cursor of its own
        size_t _emitted {0};

        bool finished() const;

        void reset();

        size_t size() const;
        void resize(size_t size);
        void clear();
        template <typename F, typename... Args>
        void apply(const F& func, Args&&... args);
    };

    GraphView _view;

    Samples _sampleData {};

    // The edges a hop's writer drew in one call, before they reach the hop's columns
    ColumnNodeIDs _drawnSrcs;
    ColumnNodeIDs _drawnTgts;

    std::optional<uint64_t> _rngSeed;

    bool _started {false};

    void pushNode(HopData& data, NodeID node);
    void pushFrontier(size_t hop, const ColumnNodeIDs* nodes);
    size_t emitFrontier(size_t hop, size_t maxRows);
    size_t expandHop(size_t hop, size_t maxRows);
};

}
