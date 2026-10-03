// WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i MATCH (m)-[e]->(n:Person) WHERE n = ss[i] RETURN m.name, n.name
func.func @main() {
  %0 = db.constant([0, 1, 6, 99])
  %1 = db.constant(0 : i64)
  %2 = db.constant(3 : i64)
  %3 = db.range(%1, %2) : (!db.column<i64>, !db.column<i64>) -> !db.column<!storage.list<i64>>
  %element = db.unwind(%3, {}) : (!db.column<!storage.list<i64>>) -> !db.column<none>
  %4 = db.list_index %0, %element : (!db.column<!storage.list<i64>>, !db.column<none>) -> !db.column<none>
  %nodes = db.fetch_nodes(%4, {}) : (!db.column<none>) -> !db.column<!storage.node_id>
  %5 = db.get_node_label_set(%nodes) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %6 = db.check_label_constraint(%5, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %7 = db.filter(%6, {%nodes}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %8, %9, %10, %11 = db.get_in_edges(%7, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %12 = db.get_node_properties(%8, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %13 = db.get_node_properties(%11, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%12, %13) names ["m.name", "n.name"] : !db.column<none>, !db.column<none>
  return
}
