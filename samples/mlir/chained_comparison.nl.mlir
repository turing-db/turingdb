// MATCH (n) RETURN 30 < n.age < 33
func.func @main() {
  %0 = nl.constant(33)
  %1 = nl.get_property_type("age")
  %2 = nl.constant(30)
  %3 = nl.scan_nodes()
  nl.for %arg0 in %3 {
    %4 = nl.get_node_properties(%arg0, %1)
    %5 = nl.lt %2, %4
    %6 = nl.lt %4, %0
    %7 = nl.and %5, %6
    nl.output(%7) names ["30 < n.age < 33"]
  }
  return
}
