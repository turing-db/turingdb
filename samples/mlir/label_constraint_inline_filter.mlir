// MATCH (n:Person{name:'Cyrus'}) RETURN n
func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_properties(%0, "name")
  %2 = db.constant("Cyrus")
  %3 = db.eq %1, %2
  %4 = db.filter(%3, {%0})
  %5 = db.get_node_label_set(%4)
  %6 = db.check_label_constraint(%5, ["Person"])
  %7 = db.filter(%6, {%4})
  db.output(%7)
  return
}
