// MATCH (s:Founder) WITH s MATCH (m)-[e:KNOWS_WELL]->(n) WHERE n = s RETURN m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["KNOWS_WELL"])
  %2 = nl.scan_nodes_by_label(["Founder"])
  nl.for %arg0 in %2 {
    %nodes = nl.fetch_nodes(%arg0, {})
    %3 = nl.get_in_edges_by_type(%nodes, %1, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %3 {
      %4 = nl.get_node_properties(%arg1, %0)
      nl.output(%4) names ["m.name"]
    }
  }
  return
}
