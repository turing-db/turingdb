// MATCH (n:Person) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = nl.constant(2)
  %1 = nl.constant(0)
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 {
    %3 = nl.eq %arg0, %1
    %4 = nl.eq %arg0, %0
    %5 = nl.or %3, %4
    %6 = nl.filter %5, (%arg0)
    nl.output(%6) names ["n"]
  }
  return
}
