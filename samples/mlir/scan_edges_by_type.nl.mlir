// Generated nl-dialect lowering of scan_edges_by_type.mlir (db dialect).
// Reproduce with: mlir -dump-lowered scan_edges_by_type.mlir
// This is the DBLowering output; edit scan_edges_by_type.mlir, not this file.
//
// db.scan_edges_by_type lowers to an nl.scan_edges_by_type source op whose
// nl.for binds the same four edge chunks nl.scan_edges binds - source, edge,
// edge type, target. The type name is hoisted above the loop into the
// nl.get_edge_type handle the by-type hops share, so it is resolved once.
module {
  func.func @main() {
    %0 = nl.get_edge_type("KNOWS_WELL")
    %1 = nl.scan_edges_by_type(%0)
    nl.for %arg0, %arg1, %arg2, %arg3 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%arg0, %arg3) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
    return
  }
}
