#pragma once

#include <stddef.h>

#include "columns/ColumnIDs.h"
#include "iterators/NeighbourhoodSampleIterator.h"

#include "columns/ColumnOptVector.h"

namespace db {

class GraphSAGESampler {
public:
    constexpr static size_t hops = 3;
    using Fanouts = std::array<size_t, hops>;

    explicit GraphSAGESampler(const GraphView* view, const ColumnNodeIDs* seeds,
                              Fanouts fanouts);

    void sample();

private:
    const GraphView* _view {nullptr};
    const ColumnNodeIDs* _seeds {nullptr};

    std::array<size_t, 3> _fanouts;

    size_t _requiredLength {0};

    ColumnOptVector<NodeID>* _srcs1 {nullptr};
    ColumnOptVector<NodeID>* _tgts1 {nullptr};
    ColumnOptVector<NodeID>* _dstNodes1 {nullptr}; // @ref _seeds, null extended

    ColumnOptVector<NodeID>* _srcs2 {nullptr};
    ColumnOptVector<NodeID>* _tgts2 {nullptr};
    // TODO: check whether dst_nodes should include srcs of previous
    // i.e. unique(seeds_k ∪ tgts_k)
    ColumnOptVector<NodeID>* _dstNodes2 {nullptr};

    ColumnOptVector<NodeID>* _srcs3 {nullptr};
    ColumnOptVector<NodeID>* _tgts3 {nullptr};
    ColumnOptVector<NodeID>* _dstNodes3 {nullptr};
};

}
