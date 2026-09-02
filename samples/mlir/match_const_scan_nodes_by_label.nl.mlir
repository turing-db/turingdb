// MATCH (n:Person) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = nl.const_scan_nodes([0, 2])
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %1 = nl.get_node_label_set(%arg0) : !nl.chunk<!storage.labelset_id>
    %2 = nl.check_label_constraint(%1, [0, 1, 6, 7, 9]) : !nl.chunk<!storage.bool>
    %3 = nl.filter %2, (%arg0) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%3) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
