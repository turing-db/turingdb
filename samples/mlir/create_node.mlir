// CREATE (n:Person)
func.func @main() {
  %0 = db.create_node(["Person"], [], {}) : () -> !db.column<!storage.node_id>
  return
}
