// MATCH (n) WHERE n.age = 32 RETURN n, once FuseScanByPropertyValue has fused the filter

func.func @main() {
  %0 = db.scan_nodes_by_property_value("age", 32)
  db.output(%0) names ["n"]
  return
}
