// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD tgt WHERE tgt <> n RETURN n, tgt

func.func @main() {
  %0 = nl.procedure("gnn.neighbourhoodSample") yields ["tgt"]
  %1 = nl.constant(2 : i64)
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.procedure_init(%0, (%arg0, %1), {%arg0}) : (!nl.procedure_state, !nl.chunk<!storage.node_id>, !nl.chunk<i64>, !nl.chunk<!storage.node_id>) -> !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>>
    nl.for %arg1, %arg2 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
      %4 = nl.neq %arg1, %arg2 : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.chunk<i1>
      %5:2 = nl.filter %4, (%arg2, %arg1) : (!nl.chunk<i1>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
      nl.output(%5#0, %5#1) names ["n", "tgt"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  return
}
