// Generated nl-dialect lowering of vector_search_hop.mlir (db dialect).
// Reproduce with: mlir -f vector_search_hop.mlir -l
// This is the DBLowering output; edit vector_search_hop.mlir, not this file.
//
// Needs no -graph: nothing here is resolved against a schema. The search opens the outer
// loop the way a scan would, and the hop's loop nests inside it, so the traversal walks
// out of the neighbours rather than out of the graph.
module {
  func.func @main() {
    %0 = nl.vector_search("vectors", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
    nl.for %arg0, %arg1 in %0 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
      %1 = nl.get_out_edges(%arg0, {%arg1}) : !nl.chunk<!storage.nullable<f64>>
      nl.for %arg2, %arg3, %arg4, %arg5, %arg6 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
        nl.output(%arg2, %arg5, %arg6) names ["ids", "m", "score"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>
      }
    }
    return
  }
}
