// MATCH (r {name: 'Remy'}) CREATE (z:Person {name: 'Zed'})-[:KNOWS_WELL]->(r) WITH z MATCH (n)-[:KNOWS_WELL]->(m) WHERE n = z RETURN m.name
func.func @main() {
  %0 = db.constant("Zed")
  %1 = db.scan_nodes_by_property_value("name", "Remy")
  %2 = db.create_node(["Person"], ["name"], {%0}) foreach %1
  %3 = db.create_edge(%2, %1, "KNOWS_WELL", [], {}) src_all_pending
  %nodes = db.fetch_nodes(%2, {})
  %4, %5, %6, %7 = db.get_out_edges_by_type(%nodes, ["KNOWS_WELL"], {})
  %8 = db.get_node_properties(%7, "name")
  db.output(%8) names ["m.name"]
  return
}
