// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (m:Person)-[e:KNOWS_WELL]->(n) WHERE n = ids RETURN m.name, e.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %2 = nl.get_in_edges_by_label(%nodes, ["Person"], {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %3 = nl.check_edge_type_constraint(%arg4, ["KNOWS_WELL"]) : !nl.chunk<!storage.bool>
      %4:2 = nl.filter %3, (%arg2, %arg3) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>)
      %5 = nl.get_node_properties(%4#0, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      %6 = nl.get_edge_properties(%4#1, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%5, %6) names ["m.name", "e.name"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
