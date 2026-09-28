func.func @main() {
  %0 = nl.get_property_type("isFrench")
  %1 = nl.constant(32)
  %2 = nl.get_property_type("age")
  %3 = nl.constant("Cyrus")
  %4 = nl.get_property_type("name")
  %5 = nl.scan_nodes()
  nl.for %arg0 in %5 {
    %6 = nl.scan_nodes()
    nl.for %arg1 in %6 {
      %7 = nl.cross_product{%arg0} {%arg1}
      nl.for %arg2, %arg3 in %7 {
        %8 = nl.get_node_properties(%arg2, %4)
        %9 = nl.eq %8, %3
        %10 = nl.get_node_properties(%arg3, %4)
        %11 = nl.get_node_properties(%arg2, %4)
        %12 = nl.eq %10, %11
        %13 = nl.and %9, %12
        %14 = nl.get_node_properties(%arg3, %2)
        %15 = nl.eq %14, %1
        %16 = nl.and %13, %15
        %17:2 = nl.filter %16, (%arg3, %arg2)
        %18 = nl.get_node_properties(%17#1, %0)
        %19 = nl.get_node_properties(%17#0, %0)
        %20 = nl.eq %18, %19
        %21:2 = nl.filter %20, (%17#0, %17#1)
        nl.output(%21#1)
      }
    }
  }
  return
}
