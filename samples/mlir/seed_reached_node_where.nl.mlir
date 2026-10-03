// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a)-->(n) WHERE n = ids AND a.age > 20 RETURN a.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.constant(20)
  %2 = nl.get_property_type("age")
  %3 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %3 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %4 = nl.get_in_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %4 {
      %5 = nl.get_node_properties(%arg2, %2)
      %6 = nl.gt %5, %1
      %7 = nl.filter %6, (%arg2)
      %8 = nl.get_node_properties(%7, %0)
      nl.output(%8) names ["a.name"]
    }
  }
  return
}
