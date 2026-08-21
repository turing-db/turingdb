// Generated nl-dialect lowering of group_count_distinct.mlir (db dialect).
// Reproduce with: mlir -dump-lowered group_count_distinct.mlir -graph <graph>
// This is the DBLowering output; edit group_count_distinct.mlir, not this file.
//
// count_distinct lowers to the same accumulator shape as count - a hoisted
// nl.group_aggregate_buffer, an nl.group_aggregate_update inside the traversal loop,
// and an nl.group_aggregate source iterator after it whose nl.for emits one row per
// group - and the kinds array is the only place the two differ. The distinct tally is
// per-group runtime state of the accumulator, so no op below the buffer mentions it.
// The count_distinct input is the never-null node ID chunk here, and the result is a
// non-null ui64 per group, exactly as count's is.
//
// Needs -graph: the key chunk element type is resolved from the "name" property
// during lowering. name is a String, so the grouping key is a
// !storage.nullable<!storage.string> chunk.
module {
  func.func @main() {
    %0 = nl.group_aggregate_buffer keys 1 aggregates [count_distinct]
    %1 = nl.get_property_type("name")
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %5 = nl.get_node_properties(%arg1, %1) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.group_aggregate_update %0, (%5, %arg4) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>
      }
    }
    %3 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>>
    nl.for %arg0, %arg1 in %3 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>> {
      nl.output(%arg0, %arg1) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>
    }
    return
  }
}
