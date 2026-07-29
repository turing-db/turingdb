#pragma once

namespace db {

class ProcedureData;
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
    static ProcedureData* allocData();
    static void deallocData(ProcedureData* data);
    static void execute(ProcedureState* proc);
    static void registerProcedure(ProcedureNamespace* ns);
};

}
