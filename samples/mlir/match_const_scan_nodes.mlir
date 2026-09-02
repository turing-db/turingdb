// MATCH (n) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = db.const_scan_nodes([0, 2]) : !db.column<!storage.node_id>
  db.output(%0) names ["n"] : !db.column<!storage.node_id>
  return
}
