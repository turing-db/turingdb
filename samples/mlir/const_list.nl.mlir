// Generated nl-dialect lowering of const_list.mlir (db dialect).
// Reproduce with: mlir -f const_list.mlir -l
// This is the DBLowering output; edit const_list.mlir, not this file.
//
// Needs no -graph: the chunk element type comes from the literals, not from a schema.
// db.const_list lowers to an nl.const_list hoisted above the loops - a list literal is one
// value for the whole query, so it is materialized once into the query-scoped list buffer
// and the chunk holds a view of it. There is no loop here at all: the list opens no
// dataflow, and a projection of constants alone is emitted once.
module {
  func.func @main() {
    %0 = nl.const_list([1, 2, 3]) : !nl.chunk<!storage.list<i64>>
    nl.output(%0) : !nl.chunk<!storage.list<i64>>
    return
  }
}
