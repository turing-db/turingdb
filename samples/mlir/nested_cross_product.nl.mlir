// Generated nl-dialect lowering of nested_cross_product.mlir (db dialect).
// Reproduce with: mlir -dump-lowered nested_cross_product.mlir
// This is the DBLowering output; edit nested_cross_product.mlir, not this file.
//
// The three-way MATCH (a), (b), (c) becomes one loop nest: the inner
// nl.cross_product crosses each `a` chunk with each `b` chunk, and drives a loop
// over the (a, b) pairs it makes; the `c` scan re-runs inside that, and the outer
// nl.cross_product crosses the pairs with each `c` chunk, driving a loop of its
// own over the triples before nl.output. Each product emits a chunk of its result
// per step rather than all of it at once.
module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %1 = nl.scan_nodes()
      nl.for %arg1 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
        %2 = nl.cross_product{%arg0} {%arg1} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
        nl.for %arg2, %arg3 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
          %3 = nl.scan_nodes()
          nl.for %arg4 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
            %4 = nl.cross_product{%arg2, %arg3} {%arg4} : {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
            nl.for %arg5, %arg6, %arg7 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
              nl.output(%arg5, %arg6, %arg7) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
            }
          }
        }
      }
    }
    return
  }
}
