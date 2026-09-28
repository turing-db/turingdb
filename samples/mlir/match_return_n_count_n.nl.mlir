// MATCH (n) RETURN n, count(n)

func.func @main() {
  %0 = nl.group_aggregate_buffer keys 1 aggregates [count]
  %1 = nl.scan_nodes()
  nl.for %arg0 in %1 {
    nl.group_aggregate_update %0, (%arg0, %arg0)
  }
  %2 = nl.group_aggregate(%0)
  nl.for %arg0, %arg1 in %2 {
    nl.output(%arg0, %arg1)
  }
  return
}
