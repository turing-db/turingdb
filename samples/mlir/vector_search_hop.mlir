module {
  func.func @main() {
    // VECTOR SEARCH IN vectors FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids, score
    // MATCH (ids)-[e]->(m) RETURN ids, m, score: the search drives the traversal.
    //
    // A neighbour is the node the index holds it under, so the pattern can name the
    // yielded variable itself. The hop then expands this column - there is no
    // db.scan_nodes here, and no db.cross_product pairing two sides: the rows the search
    // reported are the rows the traversal walks out of.
    //
    // The score rides the hop's carry set, so it is replicated once per edge the
    // expansion walked and stays row-aligned with the neighbour it belongs to.
    //
    // Needs no -graph: nothing here is resolved against a schema.
    %ids, %scores = db.vector_search("vectors", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !db.column<!storage.node_id>, !db.column<f64>

    %0, %1, %2, %3, %4 = db.get_out_edges(%ids, {%scores}) : (!db.column<!storage.node_id>, !db.column<f64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<f64>)

    db.output(%0, %3, %4) names ["ids", "m", "score"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<f64>

    return
  }
}
