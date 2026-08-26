// Generated nl-dialect lowering of collect_distinct.mlir (db dialect).
// Reproduce with: mlir -dump-lowered collect_distinct.mlir -graph <graph>
// This is the DBLowering output; edit collect_distinct.mlir, not this file.
//
// The flag rides the accumulator: nl.collect_buffer carries `distinct`, so the fold
// nl.collect_update runs is the deduplicating one. Nothing else about the shape moves -
// the same hoisted buffer, the same update inside the loop nest, the same nl.collect
// source draining one row per group - which is why collect(DISTINCT x) is a flag rather
// than an op of its own.
//
// Needs -graph: the grouping key's chunk element type is resolved from the "name"
// property during lowering, so it is a !storage.nullable<!storage.string> chunk. The
// collected column is already a node ID, so the list cell is a
// !storage.list<!storage.node_id> chunk whatever the schema says.
module {
  func.func @main() {
    %0 = nl.collect_buffer keys 1 distinct
    %1 = nl.get_property_type("name")
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %5 = nl.get_node_properties(%arg1, %1) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.collect_update %0, (%5, %arg4) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>
      }
    }
    %3 = nl.collect(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.node_id>>>
    nl.for %arg0, %arg1 in %3 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.node_id>>> {
      nl.output(%arg0, %arg1) : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.list<!storage.node_id>>
    }
    return
  }
}
