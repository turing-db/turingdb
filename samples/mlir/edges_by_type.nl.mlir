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
