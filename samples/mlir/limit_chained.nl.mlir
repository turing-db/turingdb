// Generated nl-dialect lowering of limit_chained.mlir (db dialect).
// Reproduce with: mlir -dump-lowered limit_chained.mlir
// This is the DBLowering output; edit limit_chained.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.limit(2)
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 limit %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.limit_update %0, %arg0 : !nl.chunk<!storage.node_id>
      %2 = nl.limit_truncate %0, (%arg0) : !nl.chunk<!storage.node_id>
      %3 = nl.get_out_edges(%2, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%arg4) : !nl.chunk<!storage.node_id>
      }
    }
    return
  }
}
