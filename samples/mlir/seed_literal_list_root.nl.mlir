// UNWIND [0, 6, 99] AS x MATCH (n)-[e:INTERESTED_IN]->(m) WHERE n = x RETURN n.name, m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.const_scan_nodes([0, 6, 99])
  nl.for %arg0 in %2 {
    %3 = nl.get_out_edges_by_type(%arg0, %1, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %3 {
      %4 = nl.get_node_properties(%arg1, %0)
      %5 = nl.get_node_properties(%arg4, %0)
      nl.output(%4, %5) names ["n.name", "m.name"]
    }
  }
  return
}
