// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n) WHERE n = ids AND n.name <> 'Adam' RETURN n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0 = db.get_node_properties(%nodes, "name")
  %1 = db.constant("Adam")
  %2 = db.neq %0, %1
  %3 = db.filter(%2, {%0})
  db.output(%3) names ["n.name"]
  return
}
