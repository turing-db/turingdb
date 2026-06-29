#pragma once

namespace db {

class ProcedureData;
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
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
