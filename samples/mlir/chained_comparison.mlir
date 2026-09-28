// MATCH (n) RETURN 30 < n.age < 33
func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.constant(30)
  %2 = db.get_node_properties(%0, "age")
  %3 = db.lt %1, %2
  %4 = db.constant(33)
  %5 = db.lt %2, %4
  %6 = db.and %3, %5
  db.output(%6) names ["30 < n.age < 33"]
  return
}
