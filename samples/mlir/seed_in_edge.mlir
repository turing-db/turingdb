// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (m)-[e]->(n:Person) WHERE n = ids RETURN m.name, n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0 = db.get_node_label_set(%nodes) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %1 = db.check_label_constraint(%0, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %2 = db.filter(%1, {%nodes}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %3, %4, %5, %6 = db.get_in_edges(%2, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %7 = db.get_node_properties(%3, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %8 = db.get_node_properties(%6, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%7, %8) names ["m.name", "n.name"] : !db.column<none>, !db.column<none>
  return
}
