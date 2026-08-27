// Generated nl-dialect lowering of vector_search.mlir (db dialect).
// Reproduce with: mlir -f vector_search.mlir -l
// This is the DBLowering output; edit vector_search.mlir, not this file.
//
// Needs no -graph: the two chunk types are fixed, so nothing here is resolved against a
// schema. db.vector_search lowers to an nl.vector_search source op whose nl.for binds the
// two neighbour chunks, each step filling them with the next slice of the search result.
//
// The neighbours ride the same !storage.node_id chunk a scan binds, so a hop expands them
// unchanged. The distances ride a !storage.nullable<f64> chunk - the shape a property fetch
// and the unwind_collect drain emit - even though a neighbour is never null: every
// value-chunk consumer (a cross product broadcast, a filter, a skip, an aggregate)
// dispatches on nullable<T>, so the uniform shape is what makes the column composable.
module {
  func.func @main() {
    %0 = nl.vector_search("vectors", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
    nl.for %arg0, %arg1 in %0 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
      nl.output(%arg0, %arg1) names ["ids", "score"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>
    }
    return
  }
}
