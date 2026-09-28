// MATCH (n) WHERE n.age = 32 RETURN n, once FuseScanByPropertyValue has fused the filter

func.func @main() {
  %0 = db.scan_nodes_by_property_value("age", 32 : i64) : !db.column<!storage.node_id>
  db.output(%0) names ["n"] : !db.column<!storage.node_id>
  return
}
