// MATCH (n) RETURN n, count(n)

func.func @main() {
  %0 = db.scan_nodes()
  %1:2 = db.group_aggregate(%0, %0) keys 1 aggregates [count]
  db.output(%1#0, %1#1)
  return
}
