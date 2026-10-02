#pragma once

#include <array>
#include <string_view>
#include <tuple>

#include "ID.h"
#include "columns/ColumnVector.h"
#include "list/ListView.h"
#include "metadata/PropertyType.h"

namespace db {

class ProcedureState;
class ProcedureNamespace;

// Backs the visualiser's /get_node_edges endpoint. For each requested node it
// yields one row:
//
//   id            NODE    - the node id
//   outgoingEdges LIST    - outgoing edges, each a nested int list
//                           [id, src, tgt, edgeTypeID]  (or [edgeID, tgtID]
//                           when returnOnlyIDs is set), truncated per edge type
//   incomingEdges LIST    - incoming edges, likewise ([id, src, tgt, typeID] /
//                           [edgeID, srcID])
//   outEdgeCounts STRING  - JSON {edgeTypeID: totalCount} over ALL out-edges
//   inEdgeCounts  STRING  - JSON {edgeTypeID: totalCount} over ALL in-edges
//
// Edge properties are intentionally omitted (no consumer reads them). Per-edge-
// type limits arrive as parallel (types, values) list args; defaultLimit applies
// to types with no explicit limit. Unknown / deleted node ids are skipped.
struct GetNodeEdgesProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<NodeID>,
        ColumnVector<ListView>,
        ColumnVector<ListView>,
        ColumnVector<types::String::Primitive>,
        ColumnVector<types::String::Primitive>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "id",
        "outgoingEdges",
        "incomingEdges",
        "outEdgeCounts",
        "inEdgeCounts",
    };
};

}
