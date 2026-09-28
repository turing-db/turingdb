module {
  func.func @main() {
    // MATCH (a)-[]->(b) RETURN DISTINCT b: scan every node, walk its out-edges,
    // and emit each distinct target once. Several sources can point at the same
    // target, so the b column has duplicates that DISTINCT removes.
    //
    // db.remove_duplicates is a streaming filter, not a pipeline breaker: it lowers
    // to a hoisted nl.distinct seen-set and an nl.distinct_filter inside the edge
    // loop that emits each step's not-yet-seen targets as a fresh chunk. There is
    // no emit loop (unlike db.sort) - the rows are filtered in place.
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    %ub = db.remove_duplicates(%b) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    db.output(%ub) : !db.column<!storage.node_id>

    return
  }
}
