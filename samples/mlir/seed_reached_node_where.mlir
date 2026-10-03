// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a)-->(n) WHERE n = ids AND a.age > 20 RETURN a.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0, %1, %2, %3 = db.get_in_edges(%nodes, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %4 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant(20 : i64)
  %6 = db.gt %4, %5 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %7 = db.filter(%6, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %8 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%8) names ["a.name"] : !db.column<none>
  return
}
