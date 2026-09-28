// MATCH (n) RETURN labels(n)

module {
  func.func @main() {
    %n = db.scan_nodes() : !db.column<!storage.node_id>

    %labels = db.labels(%n) : (!db.column<!storage.node_id>) -> !db.column<none>

    db.output(%labels) : !db.column<none>

    return
  }
}
