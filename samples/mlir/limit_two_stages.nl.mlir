// Generated nl-dialect lowering of limit_two_stages.mlir (db dialect).
// Reproduce with: mlir -dump-lowered limit_two_stages.mlir
// This is the DBLowering output; edit limit_two_stages.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.limit(2)
    %1 = nl.limit(3)
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 limit %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.limit_update %0, %arg0 : !nl.chunk<!storage.node_id>
      %3 = nl.limit_truncate %0, (%arg0) : !nl.chunk<!storage.node_id>
      %4 = nl.get_out_edges(%3, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %4 limit %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        nl.limit_update %1, %arg4 : !nl.chunk<!storage.node_id>
        nl.output(%arg4) limit %1 : !nl.chunk<!storage.node_id>
      }
    }
    return
  }
}
