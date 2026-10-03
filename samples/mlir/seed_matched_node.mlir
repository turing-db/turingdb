// MATCH (s:Founder) WITH s MATCH (m)-[e:KNOWS_WELL]->(n) WHERE n = s RETURN m.name
func.func @main() {
  %0 = db.scan_nodes_by_label(["Founder"])
  %nodes = db.fetch_nodes(%0, {})
  %1, %2, %3, %4 = db.get_in_edges_by_type(%nodes, ["KNOWS_WELL"], {})
  %5 = db.get_node_properties(%1, "name")
  db.output(%5) names ["m.name"]
  return
}
