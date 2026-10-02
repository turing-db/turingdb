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

// Backs the visualiser's /get_nodes endpoint: given a list of node ids, yields
// one row per existing node:
//
//   id           NODE    - the node id
//   labels       LIST    - the node's label names, as a list of strings
//   inEdgeCount  UINT64  - number of incoming edges
//   outEdgeCount UINT64  - number of outgoing edges
//   properties   STRING  - the node's properties, JSON-encoded {name: value}
//
// Unknown or deleted node ids are skipped.
struct GetNodesProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<NodeID>,
        ColumnVector<ListView>,
        ColumnVector<types::UInt64::Primitive>,
        ColumnVector<types::UInt64::Primitive>,
        ColumnVector<types::String::Primitive>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "id",
        "labels",
        "inEdgeCount",
        "outEdgeCount",
        "properties",
    };
};

}
