// MATCH (n) RETURN n.age, count(n), sum(n.age)

func.func @main() {
  %0 = db.scan_nodes()
  %1 = db.get_node_properties(%0, "age")
  %2 = db.get_node_properties(%0, "age")
  %3:3 = db.group_aggregate(%1, %0, %2) keys 1 aggregates [count, sum]
  db.output(%3#0, %3#1, %3#2)
  return
}
