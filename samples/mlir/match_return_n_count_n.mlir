// MATCH (n) RETURN n, count (n)

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1:2 = db.group_aggregate(%0, %0) keys 1 aggregates [count] : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%1#0, %1#1) : !db.column<!storage.node_id>, !db.column<none>
  return
}
