// MATCH (n) RETURN labels(n)

module {
  func.func @main() {
    %n = db.scan_nodes()

    %labels = db.labels(%n)

    db.output(%labels)

    return
  }
}
