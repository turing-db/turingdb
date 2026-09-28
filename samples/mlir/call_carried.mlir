// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD src, tgt RETURN n, tgt

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.constant(2 : i64)
  %2:3 = db.call_procedure("gnn.neighbourhoodSample", {%0, %1}, {%0}) yields ["src", "tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<none>, !db.column<!storage.node_id>)
  db.output(%2#2, %2#1) names ["n", "tgt"] : !db.column<!storage.node_id>, !db.column<none>
  return
}
