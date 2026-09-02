// MATCH (n) WHERE n.age = 32 RETURN n, as codegen emits it before the passes run

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.constant(32 : i64)
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %4 = db.filter(%3, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%4) names ["n"] : !db.column<!storage.node_id>
  return
}
