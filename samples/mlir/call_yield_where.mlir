// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD tgt WHERE tgt <> n RETURN n, tgt

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  %1 = db.constant(2)
  %2:2 = db.call_procedure("gnn.neighbourhoodSample", {%0, %1}, {%0}) yields ["tgt"]
  %3 = db.neq %2#0, %2#1
  %4:2 = db.filter(%3, {%2#1, %2#0})
  db.output(%4#0, %4#1) names ["n", "tgt"]
  return
}
