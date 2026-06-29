#pragma once

namespace db {

class ProcedureData;
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
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
