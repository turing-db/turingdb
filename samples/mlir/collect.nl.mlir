// Generated nl-dialect lowering of collect.mlir (db dialect).
// Reproduce with: mlir -dump-lowered collect.mlir -graph <graph>
// This is the DBLowering output; edit collect.mlir, not this file.
//
// db.collect is a pipeline breaker: a hoisted nl.collect_buffer accumulator, an
// nl.collect_update inside the scan loop that appends each chunk's values to the
// per-group lists, and - after the loop - an nl.collect source iterator whose nl.for
// emits one row per group, the key value then a list cell (a ListView over the group's
// elements).
//
// Needs -graph: the chunk element types are resolved from the "age" and "name"
// properties during lowering. age is Int64 and name is String, so the grouping key is
// a !storage.nullable<i64> chunk and the per-group list cell is a
// !storage.list<!storage.string> chunk.
module {
  func.func @main() {
    %0 = nl.collect_buffer keys 1
    %1 = nl.get_property_type("name")
    %2 = nl.get_property_type("age")
    %3 = nl.scan_nodes()
    nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %5 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<i64>>
      %6 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.collect_update %0, (%5, %6) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
    }
    %4 = nl.collect(%0) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>>
    nl.for %arg0, %arg1 in %4 : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>> {
      nl.output(%arg0, %arg1) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.list<!storage.string>>
    }
    return
  }
}
