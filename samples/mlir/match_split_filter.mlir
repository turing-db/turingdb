// MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN *

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.constant(32 : i64)
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %4 = db.filter(%3, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %5 = db.get_node_properties(%4, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
  %6 = db.constant(true)
  %7 = db.eq %5, %6 : (!db.column<none>, !db.column<i1>) -> !db.column<!storage.bool>
  %8 = db.filter(%7, {%4}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%8) names ["n"] : !db.column<!storage.node_id>
  return
}
