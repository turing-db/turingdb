// MATCH (n), (m) WHERE n.age = 32 and n.name = 'Remy' AND m.age IS NULL RETURN *

func.func @main() {
  %0 = nl.constant("")
  %1 = nl.constant("Remy")
  %2 = nl.get_property_type("name")
  %3 = nl.constant(32)
  %4 = nl.get_property_type("age")
  %5 = nl.scan_nodes()
  nl.for %arg0 in %5 {
    %6 = nl.get_node_properties(%arg0, %4)
    %7 = nl.eq %6, %3
    %8 = nl.filter %7, (%arg0)
    %9 = nl.get_node_properties(%8, %2)
    %10 = nl.eq %9, %1
    %11 = nl.filter %10, (%8)
    %12 = nl.scan_nodes()
    nl.for %arg1 in %12 {
      %13 = nl.get_node_properties(%arg1, %4)
      %14 = nl.eq %13, %0
      %15 = nl.filter %14, (%arg1)
      %16 = nl.cross_product{%11} {%15}
      nl.for %arg2, %arg3 in %16 {
        nl.output(%arg2, %arg3) names ["n", "m"]
      }
    }
  }
  return
}
