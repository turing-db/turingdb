// MATCH (n) RETURN n.age, count(n), sum(n.age)

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %3:3 = db.group_aggregate(%1, %0, %2) keys 1 aggregates [count, sum] : (!db.column<none>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<none>, !db.column<none>, !db.column<none>)
  db.output(%3#0, %3#1, %3#2) : !db.column<none>, !db.column<none>, !db.column<none>
  return
}
