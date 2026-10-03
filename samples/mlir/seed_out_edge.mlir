// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e:INTERESTED_IN]->(m) WHERE n = ids RETURN n.name, m.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2, %3 = db.get_out_edges_by_type(%nodes, ["INTERESTED_IN"], {})
  %4 = db.get_node_properties(%0, "name")
  %5 = db.get_node_properties(%3, "name")
  db.output(%4, %5) names ["n.name", "m.name"]
  return
}
