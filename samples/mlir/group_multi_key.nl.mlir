// Generated nl-dialect lowering of group_multi_key.mlir (db dialect).
// Reproduce with: mlir -dump-lowered group_multi_key.mlir -graph <graph>
// This is the DBLowering output; edit group_multi_key.mlir, not this file.
//
// db.group_aggregate is a pipeline breaker like db.sort: a hoisted
// nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update inside the
// scan loop that folds each chunk into the per-group state, and - after the loop -
// an nl.group_aggregate source iterator whose nl.for emits one row per group.
// Grouping on two keys is just keys 2 on the buffer: the group identity is the
// (team, city) tuple, and the emit iterator carries both key columns before the
// single aggregate result column.
//
// Needs -graph: the chunk element types are resolved from the "team", "city" and
// "score" properties during lowering. team and city are Strings and score is
// Int64, so the two keys are !storage.nullable<!storage.string> chunks and the sum
// keeps score's i64.
module {
  func.func @main() {
    %0 = nl.group_aggregate_buffer keys 2 aggregates [1]
    %1 = nl.get_property_type("score")
    %2 = nl.get_property_type("city")
    %3 = nl.get_property_type("team")
    %4 = nl.scan_nodes()
    nl.for %arg0 in %4 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %6 = nl.get_node_properties(%arg0, %3) : !nl.chunk<!storage.nullable<!storage.string>>
      %7 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
      %8 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
      nl.group_aggregate_update %0, (%6, %7, %8) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>
    }
    %5 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>>
    nl.for %arg0, %arg1, %arg2 in %5 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>> {
      nl.output(%arg0, %arg1, %arg2) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>
    }
    return
  }
}
