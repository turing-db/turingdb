// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[*1..2]->(m) WHERE n = ids RETURN n.name, m.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00])
  %nodes = db.fetch_nodes(%ids, {})
  %0, %1, %2 = db.explore_paths(%nodes, {}) forward hops 1 to 2
  %3 = db.get_node_properties(%0, "name")
  %4 = db.get_node_properties(%1, "name")
  db.output(%3, %4) names ["n.name", "m.name"]
  return
}
