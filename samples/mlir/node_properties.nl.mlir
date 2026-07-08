// Generated nl-dialect lowering of node_properties.mlir (db dialect).
// Reproduce with: mlir -dump-lowered node_properties.mlir -graph <graph>
// This is the DBLowering output; edit node_properties.mlir, not this file.
//
// Needs -graph: the value chunk element type is resolved from the "name"
// property during lowering. In simpledb name is a String, so the fetch produces
// a !storage.nullable<!storage.string> value chunk in place inside the scan
// loop; the node IDs pass straight through, and both columns feed one nl.output.
module {
  func.func @main() {
    %0 = nl.get_property_type("name")
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.get_node_properties(%arg0, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%arg0, %2) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>
    }
    return
  }
}
