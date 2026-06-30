// Generated nl-dialect lowering of skip.mlir (db dialect).
// Reproduce with: mlir -dump-lowered skip.mlir
// This is the DBLowering output; edit skip.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.skip(3)
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!nl.node_id>> {
      nl.skip_update %0, %arg0 : !nl.chunk<!nl.node_id>
      nl.output(%arg0) skip %0 : !nl.chunk<!nl.node_id>
    }
    return
  }
}
