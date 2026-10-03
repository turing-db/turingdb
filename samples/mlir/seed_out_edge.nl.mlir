// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e:INTERESTED_IN]->(m) WHERE n = ids RETURN n.name, m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %2 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %3 = nl.get_out_edges_by_type(%nodes, %1, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %3 {
      %4 = nl.get_node_properties(%arg2, %0)
      %5 = nl.get_node_properties(%arg5, %0)
      nl.output(%4, %5) names ["n.name", "m.name"]
    }
  }
  return
}
