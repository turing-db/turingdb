// Generated nl-dialect lowering of nested_cross_product.mlir (db dialect).
// Reproduce with: mlir -dump-lowered nested_cross_product.mlir
// This is the DBLowering output; edit nested_cross_product.mlir, not this file.
//
// The three-way MATCH (a), (b), (c) becomes one loop nest: the inner
// nl.cross_product crosses each `a` chunk with each `b` chunk into the (a, b)
// pairs, the `c` scan then re-runs inside that, and the outer nl.cross_product
// crosses the (a, b) pairs with each `c` chunk before nl.output.
module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %1 = nl.scan_nodes()
      nl.for %arg1 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
        %2:2 = nl.cross_product{%arg0} {%arg1} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
        %3 = nl.scan_nodes()
        nl.for %arg2 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
          %4:3 = nl.cross_product{%2#0, %2#1} {%arg2} : {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
          nl.output(%4#0, %4#1, %4#2) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
        }
      }
    }
    return
  }
}
