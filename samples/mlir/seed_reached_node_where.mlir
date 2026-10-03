// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a)-->(n) WHERE n = ids AND a.age > 20 RETURN a.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2, %3 = db.get_in_edges(%nodes, {})
  %4 = db.get_node_properties(%0, "age")
  %5 = db.constant(20)
  %6 = db.gt %4, %5
  %7 = db.filter(%6, {%0})
  %8 = db.get_node_properties(%7, "name")
  db.output(%8) names ["a.name"]
  return
}
