// Generated nl-dialect lowering of branch.mlir (db dialect).
// Reproduce with: mlir -dump-lowered branch.mlir
// This is the DBLowering output; edit branch.mlir, not this file.
//
// MATCH (a)->(b)->(c), (b)->(d) RETURN a. Three nested loops: the branch loop
// nests inside the second hop's loop, because its input `b` (%arg5, the second
// hop's srcids) is bound there - the branch leaves from `b`, not from the chain
// tip `c`. `a` rides the carry set through both hops, so the emitted %arg14 is
// filtered/replicated to one row per (a, b, c, d) match.
module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      // First hop a->b, empty carry set. %arg1 is `a`, %arg4 is `b`.
      %1 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        // Second hop b->c carrying `a` (%arg1). %arg5 is `b`, %arg8 is `c`,
        // %arg9 is the carried `a`.
        %2 = nl.get_out_edges(%arg4, {%arg1}) : !nl.chunk<!storage.node_id>
        nl.for %arg5, %arg6, %arg7, %arg8, %arg9 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
          // Branch hop b->d. Input is %arg5 (the `b` of the second hop), carry
          // set {%arg9, %arg8} = {a, c}. Carried columns come back in order, so
          // %arg14 is `a` and %arg15 is `c`, replicated per branch edge.
          %3 = nl.get_out_edges(%arg5, {%arg9, %arg8}) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
          nl.for %arg10, %arg11, %arg12, %arg13, %arg14, %arg15 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
            // RETURN a: the branch-filtered `a`, one row per (a, b, c, d).
            nl.output(%arg14) : !nl.chunk<!storage.node_id>
          }
        }
      }
    }
    return
  }
}
