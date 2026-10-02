#pragma once

#include <array>
#include <string_view>
#include <tuple>

#include "ID.h"
#include "columns/ColumnVector.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

// Yields one row per sampled edge: for each node in the input column it draws
// up to sampleSize outgoing neighbours using reservoir sampling (Algorithm L),
// then emits (src, edge, edgeType, dst) for every edge in the sample.
//
//   node        NODE   - source node ID column (e.g. from MATCH (n))
//   sampleSize  INT64  - number of neighbours to sample per node
//
//   src       NODE          - source node ID
//   edge      EDGE          - edge ID
//   edgeType  EDGE_TYPE_ID  - edge type ID
//   dst       NODE          - destination node ID
struct GnnNeighbourhoodSampleProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<NodeID>,
        ColumnVector<EdgeID>,
        ColumnVector<EdgeTypeID>,
        ColumnVector<NodeID>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "src",
        "edge",
        "edgeType",
        "tgt",
    };
};

}
