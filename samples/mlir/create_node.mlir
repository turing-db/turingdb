// CREATE (n:Person)
func.func @main() {
  %0 = db.create_node(["Person"], [], {})
  return
}
