// MATCH (n) RETURN n, count(n)

func.func @main() {
  %0 = nl.group_aggregate_buffer keys 1 aggregates [count]
  %1 = nl.scan_nodes()
  nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.group_aggregate_update %0, (%arg0, %arg0) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
  }
  %2 = nl.group_aggregate(%0) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<ui64>>
  nl.for %arg0, %arg1 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<ui64>> {
    nl.output(%arg0, %arg1) : !nl.chunk<!storage.node_id>, !nl.chunk<ui64>
  }
  return
}
