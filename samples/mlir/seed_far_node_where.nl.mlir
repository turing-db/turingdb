// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e]->(m) WHERE n = ids AND m.name <> 'Adam' RETURN n.name, m.name
func.func @main() {
  %0 = nl.constant("Adam")
  %1 = nl.get_property_type("name")
  %2 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %2 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %3 = nl.get_out_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %3 {
      %4 = nl.get_node_properties(%arg5, %1)
      %5 = nl.neq %4, %0
      %6:2 = nl.filter %5, (%arg2, %4)
      %7 = nl.get_node_properties(%6#0, %1)
      nl.output(%7, %6#1) names ["n.name", "m.name"]
    }
  }
  return
}
