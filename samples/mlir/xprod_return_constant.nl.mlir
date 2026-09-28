func.func @main() {
  %0 = nl.constant(5)
  %1 = nl.scan_nodes()
  nl.for %arg0 in %1 {
    %2 = nl.scan_nodes()
    nl.for %arg1 in %2 {
      %3 = nl.cross_product{%arg0} {%arg1}
      nl.for %arg2, %arg3 in %3 {
        nl.output(%0) cardinality(%arg2)
      }
    }
  }
  return
}
