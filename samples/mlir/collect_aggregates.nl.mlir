// Generated nl-dialect lowering of collect_aggregates.mlir (db dialect).
// Reproduce with: mlir -dump-lowered collect_aggregates.mlir -graph <graph>
// This is the DBLowering output; edit collect_aggregates.mlir, not this file.
//
// The kinds ride the accumulator: nl.collect_buffer carries `aggregates [count, min,
// max]`, so it holds a per-group reduction beside the per-group list, and one
// nl.collect_update appends to the list and charges the three reductions from the same
// chunk. The nl.collect source then yields one chunk per column - the key, the list,
// then the three reductions - which is why the aggregates ride the collect rather than
// a db.group_aggregate of their own over the same rows.
//
// Needs -graph: every chunk element type here is resolved during lowering. "dob" and
// "name" are Strings and "age" is an Int64, so the key and the list are String-shaped
// and the reductions read a !storage.nullable<i64> chunk. count emits a non-null ui64
// and min and max keep the reduced column's nullable i64.
module {
  func.func @main() {
    %0 = nl.collect_buffer keys 1 aggregates [count, min, max]
    %1 = nl.get_property_type("age")
    %2 = nl.get_property_type("name")
    %3 = nl.get_property_type("dob")
    %4 = nl.scan_nodes()
    nl.for %arg0 in %4 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %6 = nl.get_node_properties(%arg0, %3) : !nl.chunk<!storage.nullable<!storage.string>>
      %7 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
      %8 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
      nl.collect_update %0, (%6, %7, %8, %8, %8) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
    }
    %5 = nl.collect(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>, !nl.chunk<ui64>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>>
    nl.for %arg0, %arg1, %arg2, %arg3, %arg4 in %5 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>, !nl.chunk<ui64>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>> {
      nl.output(%arg0, %arg1, %arg2, %arg3, %arg4) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.string>>, !nl.chunk<ui64>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
    }
    return
  }
}
