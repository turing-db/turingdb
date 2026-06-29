#pragma once

namespace db {

class ProcedureData;
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
// Arguments (all positional; empty lists / defaults disable the corresponding
// filter):
//
//   labels         LIST   - label names; a node must carry all of them
//   propertyKeys   LIST   - property names to filter on (parallel to values)
//   propertyValues LIST   - substring queries (parallel to keys)
//   skip           INT64  - rows to skip
//   limit          INT64  - max rows to return
struct ListNodesProcedure {
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
