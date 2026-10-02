#pragma once

#include <array>
#include <string_view>
#include <tuple>

#include "ID.h"
#include "columns/ColumnVector.h"
#include "metadata/PropertyType.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

// Backs the visualiser's /get_edges endpoint: given a list of edge ids, yields
// one row per existing edge:
//
//   id         EDGE          - the edge id
//   src        NODE          - source node id
//   tgt        NODE          - target node id
//   edgeTypeID EDGE_TYPE_ID  - the edge type id
//   properties STRING        - the edge's properties, JSON-encoded {ptID: value}
//
// Unknown or deleted edge ids are skipped.
struct GetEdgesProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<EdgeID>,
        ColumnVector<NodeID>,
        ColumnVector<NodeID>,
        ColumnVector<EdgeTypeID>,
        ColumnVector<types::String::Primitive>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "id",
        "src",
        "tgt",
        "edgeTypeID",
        "properties",
    };
};

}
