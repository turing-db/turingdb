// Generated nl-dialect lowering of collect_multi.mlir (db dialect).
// Reproduce with: mlir -dump-lowered collect_multi.mlir -graph <graph>
// This is the DBLowering output; edit collect_multi.mlir, not this file.
//
// Both lists ride the accumulator: nl.collect_buffer holds a per-group value buffer per
// collected column, so one nl.collect_update appends a row's name and its age from the
// same chunk, and the nl.collect source yields one chunk per column - the key, then a
// list per value. That is what keeps a second collect out of a drain loop of its own,
// which the single nl.output could not read from.
//
// `distinct [1]` names the deduping value column by index, so the ages take each of a
// group's values once while the names beside them keep every row.
//
// Needs -graph: every chunk element type here is resolved during lowering. "dob" and
// "name" are Strings and "age" is an Int64, so the key and the first list are
// String-shaped and the second list collects i64.
module {
  func.func @main() {
    %0 = nl.collect_buffer keys 1 distinct [1]
    %1 = nl.get_property_type("age")
    %2 = nl.get_property_type("name")
    %3 = nl.get_property_type("dob")
    %4 = nl.scan_nodes()
    nl.for %arg0 in %4 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %6 = nl.get_node_properties(%arg0, %3) : !nl.chunk<!storage.nullable<!storage.string>>
      %7 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
      %8 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
      nl.collect_update %0, (%6, %7, %8) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>
    }
    %5 = nl.collect(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>, !nl.chunk<!storage.list<i64>>>
    nl.for %arg0, %arg1, %arg2 in %5 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>, !nl.chunk<!storage.list<i64>>> {
      nl.output(%arg0, %arg1, %arg2) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>, !nl.chunk<!storage.list<i64>>
    }
    return
  }
}
