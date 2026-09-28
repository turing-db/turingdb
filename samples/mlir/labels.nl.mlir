// MATCH (n) RETURN labels(n)

module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %1 = nl.labels %arg0 : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.string>
      nl.output(%1) : !nl.chunk<!storage.string>
    }
    return
  }
}
