func.func @main() {
  %c = nl.constant(5 : i64)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes : !nl.iter<!nl.chunk<!storage.node_id>> {
    nl.output(%c) cardinality(%a : !nl.chunk<!storage.node_id>) : !nl.chunk<i64>
  }
  func.return
}
