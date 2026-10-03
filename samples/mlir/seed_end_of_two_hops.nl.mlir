// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a:Person)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(n) WHERE n = ids RETURN a.name, b.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  nl.for %arg0, %arg1 in %2 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %3 = nl.get_in_edges_by_type(%nodes, %1, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %3 {
      %4 = nl.get_in_edges_by_label(%arg2, ["Person"], {})
      nl.for %arg6, %arg7, %arg8, %arg9 in %4 {
        %5 = nl.check_edge_type_constraint(%arg8, ["KNOWS_WELL"])
        %6:2 = nl.filter %5, (%arg9, %arg6)
        %7 = nl.get_node_properties(%6#1, %0)
        %8 = nl.get_node_properties(%6#0, %0)
        nl.output(%7, %8) names ["a.name", "b.name"]
      }
    }
  }
  return
}
