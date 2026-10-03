// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n) WHERE n = ids AND n.name <> 'Adam' RETURN n.name
func.func @main() {
  %0 = nl.constant("Adam")
  %1 = nl.get_property_type("name")
  %2 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %2 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %3 = nl.get_node_properties(%nodes, %1)
    %4 = nl.neq %3, %0
    %5 = nl.filter %4, (%3)
    nl.output(%5) names ["n.name"]
  }
  return
}
