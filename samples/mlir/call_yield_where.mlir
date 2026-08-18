// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD tgt WHERE tgt <> n RETURN n, tgt

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.constant(2 : i64)
  %2:2 = db.call_procedure("gnn.neighbourhoodSample", {%0, %1}, {%0}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.node_id>)
  %3 = db.neq %2#0, %2#1 : (!db.column<none>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
  %4:2 = db.filter(%3, {%2#1, %2#0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.node_id>, !db.column<none>)
  db.output(%4#0, %4#1) names ["n", "tgt"] : !db.column<!storage.node_id>, !db.column<none>
  return
}
