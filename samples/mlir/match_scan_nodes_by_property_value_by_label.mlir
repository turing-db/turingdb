// MATCH (n:Person) WHERE n.age = 32 RETURN n, once FuseScanByPropertyValue has fused the filter:
// the scan walks only the property ranges of the label sets carrying Person

func.func @main() {
  %0 = db.scan_nodes_by_property_value("age", 32, ["Person"])
  db.output(%0) names ["n"]
  return
}
