module {
  func.func @main() {
    // MATCH (a)->(b)->(c): scan `a`, then two get_out_edges hops.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    // First hop a->b, nothing carried yet so the carry set is `{}`.
    %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Second hop b->c, carrying %a1 so it comes back as %a2 (the `a` that reach a `c`).
    %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)

    // Project the (a, b, c) triple the query returns.
    db.output(%a2, %b2, %c) : !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
