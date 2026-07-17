// Generated nl-dialect lowering of unwind_collect_roundtrip.mlir (db dialect).
// Reproduce with: mlir -dump-lowered unwind_collect_roundtrip.mlir -graph <graph>
// This is the DBLowering output; edit unwind_collect_roundtrip.mlir, not this file.
//
// The ungrouped (keys 0) collect -> unwind round trip: a hoisted nl.collect_buffer, an
// nl.collect_update inside the scan loop, and an nl.unwind_collect source draining the single
// group element by element. With no grouping key the emit iterator carries only the
// value chunk, so the loop binds one variable and emits one row per collected name -
// the whole scan collapsed into one list, then expanded straight back.
//
// Needs -graph: name is resolved to a String, so the unwound value is a
// !storage.nullable<!storage.string> chunk.
module {
  func.func @main() {
    %0 = nl.collect_buffer keys 0
    %1 = nl.get_property_type("name")
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.collect_update %0, (%4) : !nl.chunk<!storage.nullable<!storage.string>>
    }
    %3 = nl.unwind_collect(%0) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>>
    nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>> {
      nl.output(%arg0) : !nl.chunk<!storage.nullable<!storage.string>>
    }
    return
  }
}
