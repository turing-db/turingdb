// MATCH (n) RETURN labels(n)

module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 {
      %1 = nl.labels %arg0
      nl.output(%1)
    }
    return
  }
}
