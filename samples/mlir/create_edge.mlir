// CREATE (n:Person)-[e:LIKES]->(i:Interest)
func.func @main() {
  %0 = db.create_node(["Person"], [], {}) : () -> !db.column<!storage.node_id>
  %1 = db.create_node(["Interest"], [], {}) : () -> !db.column<!storage.node_id>
  %2 = db.create_edge(%0, %1, "LIKES", [], {}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.edge_id>
  return
}
