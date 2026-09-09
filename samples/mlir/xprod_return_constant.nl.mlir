func.func @main() {
  %0 = nl.constant(5 : i64)
  %1 = nl.scan_nodes()
  nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %2 = nl.scan_nodes()
    nl.for %arg1 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %3 = nl.cross_product{%arg0} {%arg1} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
      nl.for %arg2, %arg3 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
        nl.output(%0) cardinality(%arg2 : !nl.chunk<!storage.node_id>) : !nl.chunk<i64>
      }
    }
  }
  return
}
