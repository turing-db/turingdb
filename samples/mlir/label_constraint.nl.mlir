// MATCH (n:Person) RETURN n
func.func @main() {
  %0 = nl.scan_nodes()
  nl.for %arg0 in %0 {
    %1 = nl.get_node_label_set(%arg0)
    %2 = nl.check_label_constraint(%1, ["Person"])
    %3 = nl.filter %2, (%arg0)
    nl.output(%3)
  }
  return
}

