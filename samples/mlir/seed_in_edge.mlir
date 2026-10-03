// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (m)-[e]->(n:Person) WHERE n = ids RETURN m.name, n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0 = db.get_node_label_set(%nodes)
  %1 = db.check_label_constraint(%0, ["Person"])
  %2 = db.filter(%1, {%nodes})
  %3, %4, %5, %6 = db.get_in_edges(%2, {})
  %7 = db.get_node_properties(%3, "name")
  %8 = db.get_node_properties(%6, "name")
  db.output(%7, %8) names ["m.name", "n.name"]
  return
}
