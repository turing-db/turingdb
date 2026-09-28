// Generated nl-dialect lowering of unwind_const.mlir (db dialect).
// Reproduce with: mlir -f unwind_const.mlir -l
// This is the DBLowering output; edit unwind_const.mlir, not this file.
//
// Needs no -graph: the chunk element type comes from the literals, not from a schema.
// db.unwind_const lowers to an nl.unwind_const source op whose nl.for binds the single
// value chunk, each step filling it with the next slice of the list.
//
// The homogeneous list rides a !storage.nullable<i64> chunk - the shape a property fetch
// and the unwind_collect drain emit - even though a literal is never null: every
// value-chunk consumer (a cross product broadcast, a filter, a skip, an aggregate)
// dispatches on nullable<T>, so the uniform shape is what makes the unwound column
// composable. A heterogeneous list keeps its !storage.list_element chunk instead.
module {
  func.func @main() {
    %0 = nl.unwind_const([1, 2, 3]) : !nl.iter<!nl.chunk<!storage.nullable<i64>>>
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
      nl.output(%arg0) : !nl.chunk<!storage.nullable<i64>>
    }
    return
  }
}
