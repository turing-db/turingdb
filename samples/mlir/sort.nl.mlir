// Generated nl-dialect lowering of sort.mlir (db dialect).
// Reproduce with: mlir -dump-lowered sort.mlir
// This is the DBLowering output; edit sort.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.sort_buffer keys [0] ascending [false]
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.sort_collect %0, (%arg0) : !nl.chunk<!storage.node_id>
    }
    %2 = nl.sort(%0) : !nl.iter<!nl.chunk<!storage.node_id>>
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.output(%arg0) : !nl.chunk<!storage.node_id>
    }
    return
  }
}
