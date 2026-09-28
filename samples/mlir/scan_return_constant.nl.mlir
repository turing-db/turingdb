func.func @main() {
  %c = nl.constant(5)
  %nodes = nl.scan_nodes()
  nl.for %a in %nodes {
    nl.output(%c) cardinality(%a)
  }
  func.return
}
