// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e:INTERESTED_IN]->(m:Interest {name: 'Ghosts'}) WHERE n = ids RETURN n.name
func.func @main() {
  %ids, %scores = db.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>
  %nodes = db.fetch_nodes(%ids, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %0, %1, %2, %3 = db.get_out_edges(%nodes, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %4 = db.get_node_properties(%3, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.constant("Ghosts" : !storage.string)
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %7:3 = db.filter(%6, {%0, %3, %2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
  %8 = db.check_edge_type_constraint(%7#2, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
  %9:2 = db.filter(%8, {%7#0, %7#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %10 = db.get_node_label_set(%9#1) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %11 = db.check_label_constraint(%10, ["Interest"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %12 = db.filter(%11, {%9#0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %13 = db.get_node_properties(%12, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%13) names ["n.name"] : !db.column<none>
  return
}
