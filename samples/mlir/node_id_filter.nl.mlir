// MATCH (n) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = nl.constant(2 : i64)
  %1 = nl.constant(0 : i64)
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.eq %arg0, %1 : (!nl.chunk<!storage.node_id>, !nl.chunk<i64>) -> !nl.chunk<i1>
    %4 = nl.eq %arg0, %0 : (!nl.chunk<!storage.node_id>, !nl.chunk<i64>) -> !nl.chunk<i1>
    %5 = nl.or %3, %4 : (!nl.chunk<i1>, !nl.chunk<i1>) -> !nl.chunk<i1>
    %6 = nl.filter %5, (%arg0) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%6) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
