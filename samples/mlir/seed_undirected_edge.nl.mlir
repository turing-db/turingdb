// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e]-(m) WHERE n = ids RETURN n.name, m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %1 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %2 = nl.get_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %2 {
      %3 = nl.get_node_properties(%arg2, %0)
      %4 = nl.get_node_properties(%arg5, %0)
      nl.output(%3, %4) names ["n.name", "m.name"]
    }
  }
  return
}
