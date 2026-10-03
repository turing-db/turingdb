// UNWIND [0, 6, 99] AS x MATCH (m)-[e]->(n:Person) WHERE n = x RETURN m.name, n.name
func.func @main() {
  %0 = db.unwind_const([0, 6, 99]) : !db.column<i64>
  %nodes = db.fetch_nodes(%0, {}) : (!db.column<i64>) -> !db.column<!storage.node_id>
  %1 = db.get_node_label_set(%nodes) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %2 = db.check_label_constraint(%1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %3 = db.filter(%2, {%nodes}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %4, %5, %6, %7 = db.get_in_edges(%3, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %8 = db.get_node_properties(%4, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %9 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%8, %9) names ["m.name", "n.name"] : !db.column<none>, !db.column<none>
  return
}
