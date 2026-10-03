// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[*1..2]->(m) WHERE n = ids RETURN n.name, m.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0, %1, %2 = db.explore_paths(%nodes, {}) forward hops 1 to 2 : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.path_ref>)
  %3 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %4 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%3, %4) names ["n.name", "m.name"] : !db.column<none>, !db.column<none>
  return
}
