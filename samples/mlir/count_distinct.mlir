module {
  func.func @main() {
    // MATCH (a)-->(b) RETURN count(DISTINCT b): scan every node, walk its out-edges
    // and count how many different nodes are pointed at. count(DISTINCT b) charges
    // each target once however many edges reach it, where count(b) would charge every
    // edge row.
    //
    // The distinct flag reuses both halves the engine already has: it lowers to the
    // DISTINCT seen-set (a hoisted nl.distinct plus an nl.distinct_filter cutting the
    // chunk in the traversal loop) feeding the ordinary nl.count_update, so the tally
    // sees each value once. A null survives the filter but is never charged, since
    // nl.count_update counts only non-null rows.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %src, %edge, %type, %tgt = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    %count = db.count(%tgt) distinct : (!db.column<!storage.node_id>) -> !db.column<ui64>

    db.output(%count) : !db.column<ui64>

    return
  }
}
