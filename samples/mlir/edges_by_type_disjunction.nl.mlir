// Generated nl-dialect lowering of edges_by_type_disjunction.mlir (db dialect).
// Reproduce with: mlir -dump-lowered edges_by_type_disjunction.mlir
// This is the DBLowering output; edit edges_by_type_disjunction.mlir, not this file.
//
// The type names are hoisted above the loops into one nl.get_edge_type_set handle, so
// they are resolved once rather than per step, exactly as a single type is.
module {
  func.func @main() {
    %0 = nl.get_edge_type_set(["KNOWS_WELL", "INTERESTED_IN"])
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.get_out_edges_by_type(%arg0, %0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%arg1, %arg4) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
      }
    }
    return
  }
}
