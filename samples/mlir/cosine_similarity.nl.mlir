// MATCH (n) RETURN cosine_similarity(n.vec, n.vec)

module {
  func.func @main() {
    %0 = nl.get_property_type("vec")
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.get_node_properties(%arg0, %0) : !nl.chunk<!storage.nullable<!storage.embedding>>
      %3 = nl.cosine_similarity %2, %2 : (!nl.chunk<!storage.nullable<!storage.embedding>>, !nl.chunk<!storage.nullable<!storage.embedding>>) -> !nl.chunk<!storage.nullable<f64>>
      nl.output(%3) : !nl.chunk<!storage.nullable<f64>>
    }
    return
  }
}
