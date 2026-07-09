// Generated nl-dialect lowering of edges_by_type.mlir (db dialect).
// Reproduce with: mlir -dump-lowered edges_by_type.mlir
// This is the DBLowering output; edit edges_by_type.mlir, not this file.
//
// The edge type name is not carried on the hop: lowering hoists it into one
// nl.get_edge_type handle at the top of the function (deduped per name), above
// every loop, and the hop takes that handle. So a hop nested in a loop resolves
// its type once rather than per step - the same split as nl.get_property_type and
// the property fetches. The name is resolved to an EdgeTypeID only at execution,
// so no -graph is needed to lower.
module {
  func.func @main() {
    %0 = nl.get_edge_type("KNOWS_WELL")
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.get_out_edges_by_type(%arg0, %0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%arg1, %arg4) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
      }
    }
    return
  }
}
