// MATCH (n:Person) RETURN n
func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_label_set(%0)
  %2 = db.check_label_constraint(%1, ["Person"])
  %3 = db.filter(%2, {%0})
  db.output(%3)
  return
}
