// MATCH (n) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = nl.const_scan_nodes([0, 2])
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%arg0) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
