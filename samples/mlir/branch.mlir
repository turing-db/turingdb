module {
  func.func @main() {
    // MATCH (a)->(b)->(c), (b)->(d) RETURN a
    //
    // A nested two-hop chain a->b->c with a branch b->d hanging off `b`. The
    // catch is the branch: it does NOT continue from the chain tip `c`, it
    // reconnects to `b`. And because the query returns `a`, the `a` we emit must
    // be the one filtered/replicated through *both* the chain and the branch -
    // one row per full (a, b, c, d) match - so `a` has to ride the carry set all
    // the way to the last hop. Returning the earlier `a` (%a2) instead of the
    // branch-filtered `a` (%a3) is exactly what undercounts `a`.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    // First hop a->b, nothing carried yet so the carry set is `{}`.
    // %src is `a`, %tgt is `b`.
    %src, %e0, %et0, %tgt = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Second hop b->c, carrying `a` (%src) so it comes back as %a2, filtered to
    // the `a` whose `b` has a successor `c`. %src1 is `b`, %tgt1 is `c`.
    %src1, %e1, %et1, %tgt1, %a2 = db.get_out_edges(%tgt, {%src}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)

    // Branch hop b->d. The input is %src1 - the `b` of the second hop, NOT its
    // target `c` - so the branch leaves from `b`. The carry set is {%a2, %tgt1}:
    // `a` (to return it) and `c` (to keep the earlier match bound). Each carried
    // column comes back in carry-set order, so %a3 is `a` and %c1 is `c`, both
    // replicated per branch edge - one row per (a, b, c, d). %src2 is `b`,
    // %tgt2 is `d`.
    %src2, %e2, %et2, %tgt2, %a3, %c1 = db.get_out_edges(%src1, {%a2, %tgt1}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)

    // RETURN a: the branch-filtered `a`, one row per full (a, b, c, d) match.
    db.output(%a3) : !db.column<!storage.node_id>

    return
  }
}
