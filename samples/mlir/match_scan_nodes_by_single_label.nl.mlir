// MATCH (n:Person) RETURN  *

func.func @main() {
  %0 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%arg0) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
