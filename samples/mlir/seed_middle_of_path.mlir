// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a)-->(n)-->(m) WHERE n = ids RETURN a.name, n.name, m.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2, %3 = db.get_in_edges(%nodes, {})
  %4, %5, %6, %7, %8 = db.get_out_edges(%3, {%0})
  %9 = db.get_node_properties(%8, "name")
  %10 = db.get_node_properties(%4, "name")
  %11 = db.get_node_properties(%7, "name")
  db.output(%9, %10, %11) names ["a.name", "n.name", "m.name"]
  return
}
