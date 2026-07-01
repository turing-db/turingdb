// Generated nl-dialect lowering of remove_duplicates.mlir (db dialect).
// Reproduce with: mlir -dump-lowered remove_duplicates.mlir
// This is the DBLowering output; edit remove_duplicates.mlir, not this file.
//
// db.remove_duplicates is a streaming filter, not a pipeline breaker: a hoisted
// nl.distinct seen-set, and an nl.distinct_filter inside the edge loop that emits
// each step's not-yet-seen targets as a fresh chunk. No emit loop (unlike db.sort).
module {
  func.func @main() {
    %0 = nl.distinct
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %3 = nl.distinct_filter %0, (%arg4) : !nl.chunk<!storage.node_id>
        nl.output(%3) : !nl.chunk<!storage.node_id>
      }
    }
    return
  }
}
