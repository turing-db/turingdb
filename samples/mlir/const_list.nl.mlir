// Generated nl-dialect lowering of const_list.mlir (db dialect).
// Reproduce with: mlir -f const_list.mlir -l
// This is the DBLowering output; edit const_list.mlir, not this file.
//
// Needs no -graph: the chunk element type comes from the literals, not from a schema.
// A list literal lowers the way every other constant does - to an nl.constant hoisted above
// the loops, materialized once into the query-scoped list buffer, with the chunk holding a
// view of it. There is no loop here at all: the list opens no dataflow, and a projection of
// constants alone is emitted once.
module {
  func.func @main() {
    %0 = nl.constant([1, 2, 3])
    nl.output(%0) : !nl.chunk<!storage.list<i64>>
    return
  }
}
