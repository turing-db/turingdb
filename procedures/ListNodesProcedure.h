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

// Backs the visualiser's /list_nodes endpoint. Scans nodes (optionally filtered
// by a label set and/or case-insensitive substring matches on string
// properties), applies skip/limit paging, and yields one row per matching node:
//
//   id         NODE    - the node id
//   labels     LIST    - the node's label names, as a list of strings
//   properties STRING  - the node's properties, JSON-encoded {name: value}
//
// Arguments (all positional; an empty list / map and defaults disable the
// corresponding filter):
//
//   labels         LIST   - label names; a node must carry all of them
//   properties     MAP    - property name -> substring query
//   skip           INT64  - rows to skip
//   limit          INT64  - max rows to return
struct ListNodesProcedure {
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);

    using Returns = std::tuple<
        ColumnVector<NodeID>,
        ColumnVector<ListView>,
        ColumnVector<types::String::Primitive>
    >;

    static constexpr size_t numReturns = std::tuple_size_v<Returns>;
    using ReturnNames = std::array<std::string_view, numReturns>;
    static constexpr ReturnNames _returnNames {
        "id",
        "labels",
        "properties",
    };
};

}
