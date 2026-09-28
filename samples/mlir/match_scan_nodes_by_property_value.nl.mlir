// MATCH (n) WHERE n.age = 32 RETURN n, once FuseScanByPropertyValue has fused the filter

func.func @main() {
  %0 = nl.scan_nodes_by_property_value("age", 32)
  nl.for %arg0 in %0 {
    nl.output(%arg0) names ["n"]
  }
  return
}
