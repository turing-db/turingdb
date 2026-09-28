module {
  func.func @main() {
    // MATCH (a)-[]->(b) RETURN DISTINCT b LIMIT 2: the first two distinct targets.
    // DISTINCT streams, so the LIMIT bounds it through the ordinary early-exit -
    // the producing loops carry the budget and halt once two distinct b's are
    // emitted. No top-K fusion (unlike ORDER BY ... LIMIT): the lowered nl keeps
    // the loops' `limit` operand, and an nl.limit_update charges the deduped
    // survivor count so the loops stop after two distinct rows rather than two
    // scanned rows.
    %a = db.scan_nodes()
    %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {})

    %ub = db.remove_duplicates(%b)

    %lb = db.limit(%ub) count 2

    db.output(%lb)

    return
  }
}
