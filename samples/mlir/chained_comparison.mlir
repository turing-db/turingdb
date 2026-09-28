// MATCH (n) RETURN 30 < n.age < 33
func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.constant(30 : i64)
  %2 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %3 = db.lt %1, %2 : (!db.column<i64>, !db.column<none>) -> !db.column<!storage.bool>
  %4 = db.constant(33 : i64)
  %5 = db.lt %2, %4 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %6 = db.and %3, %5 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  db.output(%6) names ["30 < n.age < 33"] : !db.column<!storage.bool>
  return
}
