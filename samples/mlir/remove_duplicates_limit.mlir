module {
  func.func @main() {
    // MATCH (a)-[]->(b) RETURN DISTINCT b LIMIT 2: the first two distinct targets.
    // DISTINCT streams, so the LIMIT bounds it through the ordinary early-exit -
    // the producing loops carry the budget and halt once two distinct b's are
    // emitted. No top-K fusion (unlike ORDER BY ... LIMIT): the lowered nl keeps
    // the loops' `limit` operand, and an nl.limit_update charges the deduped
    // survivor count so the loops stop after two distinct rows rather than two
    // scanned rows.
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    %ub = db.remove_duplicates(%b) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    %lb = db.limit(%ub) count 2 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    db.output(%lb) : !db.column<!storage.node_id>

    return
  }
}
