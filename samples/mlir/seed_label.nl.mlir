// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n:Person) WHERE n = ids RETURN n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %1 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %2 = nl.get_node_label_set(%nodes)
    %3 = nl.check_label_constraint(%2, ["Person"])
    %4 = nl.filter %3, (%nodes)
    %5 = nl.get_node_properties(%4, %0)
    nl.output(%5) names ["n.name"]
  }
  return
}
