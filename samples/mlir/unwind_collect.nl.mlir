// Generated nl-dialect lowering of unwind_collect.mlir (db dialect).
// Reproduce with: mlir -dump-lowered unwind_collect.mlir -graph <graph>
// This is the DBLowering output; edit unwind_collect.mlir, not this file.
//
// The fused collect-then-unwind. The accumulate phase is identical to collect.mlir - a
// hoisted nl.collect_buffer and an nl.collect_update inside the scan loop - but the
// drain is an nl.unwind source: it re-emits one row per collected element (the group's
// key value repeated, then one name), so the list never materializes. The emit iterator
// carries the key chunk then the element value chunk.
//
// Needs -graph: age is Int64 and name is String, so the key is a !storage.nullable<i64>
// chunk and the unwound value is a !storage.nullable<!storage.string> chunk.
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
    %4 = nl.unwind(%0) : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>>
    nl.for %arg0, %arg1 in %4 : !nl.iter<!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>> {
      nl.output(%arg0, %arg1) : !nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<!storage.string>>
    }
    return
  }
}
