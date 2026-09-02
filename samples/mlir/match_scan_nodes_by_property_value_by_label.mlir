// MATCH (n:Person) WHERE n.age = 32 RETURN n, once FuseScanByPropertyValue has fused the filter:
// the scan walks only the property ranges of the label sets carrying Person

func.func @main() {
  %0 = db.scan_nodes_by_property_value("age", 32 : i64, ["Person"]) : !db.column<!storage.node_id>
  db.output(%0) names ["n"] : !db.column<!storage.node_id>
  return
}
