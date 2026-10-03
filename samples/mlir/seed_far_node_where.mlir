// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e]->(m) WHERE n = ids AND m.name <> 'Adam' RETURN n.name, m.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2, %3 = db.get_out_edges(%nodes, {})
  %4 = db.get_node_properties(%3, "name")
  %5 = db.constant("Adam")
  %6 = db.neq %4, %5
  %7:2 = db.filter(%6, {%0, %4})
  %8 = db.get_node_properties(%7#0, "name")
  db.output(%8, %7#1) names ["n.name", "m.name"]
  return
}
