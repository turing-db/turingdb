#pragma once

namespace db {

class ProcedureData;
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
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
