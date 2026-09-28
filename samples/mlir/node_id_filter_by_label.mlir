// MATCH (n:Person) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  %1 = db.constant(0)
  %2 = db.eq %0, %1
  %3 = db.constant(2)
  %4 = db.eq %0, %3
  %5 = db.or %2, %4
  %6 = db.filter(%5, {%0})
  db.output(%6) names ["n"]
  return
}
