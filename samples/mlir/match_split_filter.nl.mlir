// MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN *

func.func @main() {
  %0 = nl.constant(true)
  %1 = nl.get_property_type("isFrench")
  %2 = nl.constant(32)
  %3 = nl.get_property_type("age")
  %4 = nl.scan_nodes()
  nl.for %arg0 in %4 {
    %5 = nl.get_node_properties(%arg0, %3)
    %6 = nl.eq %5, %2
    %7 = nl.filter %6, (%arg0)
    %8 = nl.get_node_properties(%7, %1)
    %9 = nl.eq %8, %0
    %10 = nl.filter %9, (%7)
    nl.output(%10) names ["n"]
  }
  return
}
