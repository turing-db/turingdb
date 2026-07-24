// MATCH (n) CREATE (n)-[e:COMES_BEFORE]->(m:New)

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.create_node(["New"], [], {}) : () -> !db.column<!storage.node_id>
  %2 = db.create_edge(%0, %1, "COMES_BEFORE", [], {}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.edge_id>
  return
}
