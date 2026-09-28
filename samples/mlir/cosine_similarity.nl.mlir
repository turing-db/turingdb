// MATCH (n) RETURN cosine_similarity(n.vec, n.vec)

module {
  func.func @main() {
    %0 = nl.get_property_type("vec")
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 {
      %2 = nl.get_node_properties(%arg0, %0)
      %3 = nl.cosine_similarity %2, %2
      nl.output(%3)
    }
    return
  }
}
