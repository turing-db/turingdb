// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n:Person {age: 32}) WHERE n = ids RETURN n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.constant(32)
  %2 = nl.get_property_type("age")
  %3 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %3 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %4 = nl.get_node_label_set(%nodes)
    %5 = nl.check_label_constraint(%4, ["Person"])
    %6 = nl.filter %5, (%nodes)
    %7 = nl.get_node_properties(%6, %2)
    %8 = nl.eq %7, %1
    %9 = nl.filter %8, (%6)
    %10 = nl.get_node_properties(%9, %0)
    nl.output(%10) names ["n.name"]
  }
  return
}
