// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n:Person) WHERE n = ids RETURN n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0 = db.get_node_label_set(%nodes)
  %1 = db.check_label_constraint(%0, ["Person"])
  %2 = db.filter(%1, {%nodes})
  %3 = db.get_node_properties(%2, "name")
  db.output(%3) names ["n.name"]
  return
}
