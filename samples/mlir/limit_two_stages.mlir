module {
  func.func @main() {
    // MATCH (a) WITH a LIMIT 2 MATCH (a)-->(b) WITH b LIMIT 3 RETURN b: two
    // independent limits in one query. The first bounds the scan to two nodes,
    // the second bounds the expansion to three edges - min(2 * out-degree, 3).
    //
    // Each db.limit gets its own hoisted nl.limit handle, so the budgets never
    // clobber each other. The scan loop is shared by both limits' producing
    // nests; the outer (first, in program order) limit claims it, and the second
    // limit's nl.limit_truncate still caps the expansion independently.
    %a = db.scan_nodes()

    %la = db.limit(%a) count 2

    %a1, %e0, %et0, %b = db.get_out_edges(%la, {})

    %lb = db.limit(%b) count 3

    db.output(%lb)

    return
  }
}
