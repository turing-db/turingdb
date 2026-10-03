// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (m:Person)-[e:KNOWS_WELL]->(n) WHERE n = ids RETURN m.name, e.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0, %1, %2, %3 = db.get_in_edges_by_label(%nodes, ["Person"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %4 = db.check_edge_type_constraint(%2, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %5:2 = db.filter(%4, {%0, %1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>)
  %6 = db.get_node_properties(%5#0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %7 = db.get_edge_properties(%5#1, "name") : (!db.column<!storage.edge_id>) -> !db.column<none>
  db.output(%6, %7) names ["m.name", "e.name"] : !db.column<none>, !db.column<none>
  return
}
