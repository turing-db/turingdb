// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a)-->(n)-->(m) WHERE n = ids RETURN a.name, n.name, m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %2 = nl.get_in_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %3 = nl.get_out_edges(%arg5, {%arg2}) : !nl.chunk<!storage.node_id>
      nl.for %arg6, %arg7, %arg8, %arg9, %arg10 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        %4 = nl.get_node_properties(%arg10, %0) : !nl.chunk<!storage.nullable<!storage.string>>
        %5 = nl.get_node_properties(%arg6, %0) : !nl.chunk<!storage.nullable<!storage.string>>
        %6 = nl.get_node_properties(%arg9, %0) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.output(%4, %5, %6) names ["a.name", "n.name", "m.name"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
      }
    }
  }
  return
}
