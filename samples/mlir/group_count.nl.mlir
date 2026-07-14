// Generated nl-dialect lowering of group_count.mlir (db dialect).
// Reproduce with: mlir -dump-lowered group_count.mlir -graph <graph>
// This is the DBLowering output; edit group_count.mlir, not this file.
//
// db.group_aggregate is a pipeline breaker like db.sort: a hoisted
// nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update inside the
// scan loop that folds each chunk into the per-group state, and - after the loop -
// an nl.group_aggregate source iterator whose nl.for emits one row per group. The
// count(*) input is the never-null node ID chunk, and the grouped count is a
// non-null ui64 per group.
//
// Needs -graph: the key chunk element type is resolved from the "team" property
// during lowering. team is a String, so the grouping key is a
// !storage.nullable<!storage.string> chunk.
module {
  func.func @main() {
    %0 = nl.group_aggregate_buffer keys 1 aggregates [0]
    %1 = nl.get_property_type("team")
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.group_aggregate_update %0, (%4, %arg0) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>
    }
    %3 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>>
    nl.for %arg0, %arg1 in %3 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>> {
      nl.output(%arg0, %arg1) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>
    }
    return
  }
}
