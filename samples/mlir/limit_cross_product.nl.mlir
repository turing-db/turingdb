// Generated nl-dialect lowering of limit_cross_product.mlir (db dialect).
// Reproduce with: mlir -dump-lowered limit_cross_product.mlir
// This is the DBLowering output; edit limit_cross_product.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.limit(5)
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 limit %0 : !nl.iter<!nl.chunk<!nl.node_id>> {
      %2 = nl.scan_nodes()
      nl.for %arg1 in %2 limit %0 : !nl.iter<!nl.chunk<!nl.node_id>> {
        %3:2 = nl.cross_product{%arg0} {%arg1} limit %0 : {!nl.chunk<!nl.node_id>} {!nl.chunk<!nl.node_id>}
        nl.limit_update %0, %3#0 : !nl.chunk<!nl.node_id>
        nl.output(%3#0, %3#1) limit %0 : !nl.chunk<!nl.node_id>, !nl.chunk<!nl.node_id>
      }
    }
    return
  }
}
