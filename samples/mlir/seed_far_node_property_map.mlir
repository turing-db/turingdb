// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e:INTERESTED_IN]->(m:Interest {name: 'Ghosts'}) WHERE n = ids RETURN n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2, %3 = db.get_out_edges(%nodes, {})
  %4 = db.get_node_properties(%3, "name")
  %5 = db.constant("Ghosts")
  %6 = db.eq %4, %5
  %7:3 = db.filter(%6, {%0, %3, %2})
  %8 = db.check_edge_type_constraint(%7#2, ["INTERESTED_IN"])
  %9:2 = db.filter(%8, {%7#0, %7#1})
  %10 = db.get_node_label_set(%9#1)
  %11 = db.check_label_constraint(%10, ["Interest"])
  %12 = db.filter(%11, {%9#0})
  %13 = db.get_node_properties(%12, "name")
  db.output(%13) names ["n.name"]
  return
}
