// MATCH (n) RETURN cosine_similarity(n.vec, n.vec)

module {
  func.func @main() {
    %n = db.scan_nodes()

    %vec = db.get_node_properties(%n, "vec")

    %sim = db.cosine_similarity(%vec, %vec)

    db.output(%sim)

    return
  }
}
