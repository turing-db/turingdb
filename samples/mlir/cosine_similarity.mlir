// MATCH (n) RETURN cosine_similarity(n.vec, n.vec)

module {
  func.func @main() {
    %n = db.scan_nodes() : !db.column<!storage.node_id>

    %vec = db.get_node_properties(%n, "vec") : (!db.column<!storage.node_id>) -> !db.column<none>

    %sim = db.cosine_similarity(%vec, %vec) : (!db.column<none>, !db.column<none>) -> !db.column<none>

    db.output(%sim) : !db.column<none>

    return
  }
}
