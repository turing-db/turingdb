// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e]-(m) WHERE n = ids RETURN n.name, m.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0, %1, %2, %3 = db.get_edges(%nodes, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %4 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.get_node_properties(%3, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%4, %5) names ["n.name", "m.name"] : !db.column<none>, !db.column<none>
  return
}
