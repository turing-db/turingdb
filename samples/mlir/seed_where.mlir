// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n) WHERE n = ids AND n.name <> 'Adam' RETURN n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0 = db.get_node_properties(%nodes, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %1 = db.constant("Adam" : !storage.string)
  %2 = db.neq %0, %1 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %3 = db.filter(%2, {%0}) : (!db.column<!storage.bool>, !db.column<none>) -> !db.column<none>
  db.output(%3) names ["n.name"] : !db.column<none>
  return
}
