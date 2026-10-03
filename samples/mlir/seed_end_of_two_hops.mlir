// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a:Person)-[:KNOWS_WELL]->(b)-[:INTERESTED_IN]->(n) WHERE n = ids RETURN a.name, b.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2, %3 = db.get_in_edges_by_type(%nodes, ["INTERESTED_IN"], {})
  %4, %5, %6, %7 = db.get_in_edges_by_label(%0, ["Person"], {})
  %8 = db.check_edge_type_constraint(%6, ["KNOWS_WELL"])
  %9:2 = db.filter(%8, {%7, %4})
  %10 = db.get_node_properties(%9#1, "name")
  %11 = db.get_node_properties(%9#0, "name")
  db.output(%10, %11) names ["a.name", "b.name"]
  return
}
