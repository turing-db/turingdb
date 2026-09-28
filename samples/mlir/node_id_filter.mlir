// MATCH (n) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.constant(0 : i64)
  %2 = db.eq %0, %1 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %3 = db.constant(2 : i64)
  %4 = db.eq %0, %3 : (!db.column<!storage.node_id>, !db.column<i64>) -> !db.column<!storage.bool>
  %5 = db.or %2, %4 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %6 = db.filter(%5, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%6) names ["n"] : !db.column<!storage.node_id>
  return
}
