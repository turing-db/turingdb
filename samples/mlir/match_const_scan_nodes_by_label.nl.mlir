// MATCH (n:Person) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = nl.const_scan_nodes([0, 2])
  nl.for %arg0 in %0 {
    %1 = nl.get_node_label_set(%arg0)
    %2 = nl.check_label_constraint(%1, ["Person"])
    %3 = nl.filter %2, (%arg0)
    nl.output(%3) names ["n"]
  }
  return
}
