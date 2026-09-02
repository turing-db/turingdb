// MATCH (n) WHERE n.age = 32 RETURN n, once FuseScanByPropertyValue has fused the filter

func.func @main() {
  %0 = nl.scan_nodes_by_property_value("age", 32 : i64)
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%arg0) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
