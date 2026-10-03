// UNWIND [0, 6, 99] AS x MATCH (m)-[e]->(n:Person) WHERE n = x RETURN m.name, n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.unwind_const([0, 6, 99])
  nl.for %arg0 in %1 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %2 = nl.get_node_label_set(%nodes)
    %3 = nl.check_label_constraint(%2, ["Person"])
    %4 = nl.filter %3, (%nodes)
    %5 = nl.get_in_edges(%4, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %5 {
      %6 = nl.get_node_properties(%arg1, %0)
      %7 = nl.get_node_properties(%arg4, %0)
      nl.output(%6, %7) names ["m.name", "n.name"]
    }
  }
  return
}
