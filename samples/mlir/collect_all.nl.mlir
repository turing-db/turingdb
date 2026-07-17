// Generated nl-dialect lowering of collect_all.mlir (db dialect).
// Reproduce with: mlir -dump-lowered collect_all.mlir -graph <graph>
// This is the DBLowering output; edit collect_all.mlir, not this file.
//
// The ungrouped (keys 0) form of collect: a hoisted nl.collect_buffer, an
// nl.collect_update inside the scan loop, and an nl.collect source draining the single
// group after the loop. With no grouping key the emit iterator carries only the list
// cell, so the nl.for binds one loop variable and emits exactly one row.
//
// Needs -graph: name is resolved to a String during lowering, so the list cell is a
// !storage.list<!storage.string> chunk.
module {
  func.func @main() {
    %0 = nl.collect_buffer keys 0
    %1 = nl.get_property_type("name")
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.collect_update %0, (%4) : !nl.chunk<!storage.nullable<!storage.string>>
    }
    %3 = nl.collect(%0) : !nl.iter<!nl.chunk<!storage.list<!storage.string>>>
    nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.list<!storage.string>>> {
      nl.output(%arg0) : !nl.chunk<!storage.list<!storage.string>>
    }
    return
  }
}
