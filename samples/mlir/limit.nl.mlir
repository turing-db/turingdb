// Generated nl-dialect lowering of limit.mlir (db dialect).
// Reproduce with: mlir -dump-lowered limit.mlir
// This is the DBLowering output; edit limit.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.limit(3)
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 limit %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.limit_update %0, %arg0 : !nl.chunk<!storage.node_id>
      nl.output(%arg0) limit %0 : !nl.chunk<!storage.node_id>
    }
    return
  }
}
