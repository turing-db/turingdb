// MATCH (r {name: 'Remy'}) CREATE (z:Person {name: 'Zed'})-[:KNOWS_WELL]->(r) WITH z MATCH (n)-[:KNOWS_WELL]->(m) WHERE n = z RETURN m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["KNOWS_WELL"])
  %2 = nl.constant("Zed")
  %3 = nl.scan_nodes_by_property_value("name", "Remy")
  nl.for %arg0 in %3 {
    %4 = nl.create_node ["Person"], ["name"], {%2} foreach %arg0
    %5 = nl.create_edge %4, %arg0, "KNOWS_WELL", [], {} src_all_pending
    %nodes = nl.fetch_nodes(%4, {})
    %6 = nl.get_out_edges_by_type(%nodes, %1, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %6 {
      %7 = nl.get_node_properties(%arg4, %0)
      nl.output(%7) names ["m.name"]
    }
  }
  return
}
