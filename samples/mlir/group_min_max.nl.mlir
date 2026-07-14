// Generated nl-dialect lowering of group_min_max.mlir (db dialect).
// Reproduce with: mlir -dump-lowered group_min_max.mlir -graph <graph>
// This is the DBLowering output; edit group_min_max.mlir, not this file.
//
// db.group_aggregate is a pipeline breaker like db.sort: a hoisted
// nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update inside the
// scan loop that folds each chunk into the per-group state, and - after the loop -
// an nl.group_aggregate source iterator whose nl.for emits one row per group. Both
// aggregates read the same score chunk; the emit iterator carries the key column
// then one result column per aggregate.
//
// Needs -graph: the chunk element types are resolved from the "team" and "score"
// properties during lowering. team is a String and score is Int64, so the key is a
// !storage.nullable<!storage.string> chunk; unlike avg (which widens to f64) both
// min and max keep score's i64.
module {
  func.func @main() {
    %0 = nl.group_aggregate_buffer keys 1 aggregates [2, 3]
    %1 = nl.get_property_type("score")
    %2 = nl.get_property_type("team")
    %3 = nl.scan_nodes()
    nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %5 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
      %6 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
      nl.group_aggregate_update %0, (%5, %6, %6) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
    }
    %4 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>>
    nl.for %arg0, %arg1, %arg2 in %4 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>> {
      nl.output(%arg0, %arg1, %arg2) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>
    }
    return
  }
}
