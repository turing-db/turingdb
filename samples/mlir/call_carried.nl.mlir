// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD src, tgt RETURN n, tgt

func.func @main() {
  %0 = nl.procedure("gnn.neighbourhoodSample") yields ["src", "tgt"]
  %1 = nl.constant(2)
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 {
    %3 = nl.procedure_init(%0, (%arg0, %1), {%arg0})
    nl.for %arg1, %arg2, %arg3 in %3 {
      nl.output(%arg3, %arg2) names ["n", "tgt"]
    }
  }
  return
}
