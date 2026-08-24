// Generated nl-dialect lowering of const_list_mixed.mlir (db dialect).
// Reproduce with: mlir -f const_list_mixed.mlir -l
// This is the DBLowering output; edit const_list_mixed.mlir, not this file.
//
// Needs no -graph: the chunk element type comes from the literals, not from a schema.
// The lowering is const_list.mlir's, unchanged by the elements disagreeing: one nl.constant
// hoisted above the loops, materialized once into the query-scoped list buffer, the chunk
// holding a view of it. Only the inferred element type differs - the erased tagged scalar
// rather than i64 - and nothing below the dialects reads it: every nl consumer tests for a
// list chunk and stops.
module {
  func.func @main() {
    %0 = nl.constant([1, "Hello", [1]])
    nl.output(%0) : !nl.chunk<!storage.list<!storage.list_element>>
    return
  }
}
