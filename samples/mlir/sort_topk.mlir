module {
  func.func @main() {
    // MATCH (a) RETURN a ORDER BY a DESC LIMIT 2: the top-2 nodes by descending
    // ID. The db.limit capping a db.sort's result is the ORDER BY ... LIMIT k
    // shape, so DBLowering fuses them: the count is baked into nl.sort_buffer as a
    // top-K bound and the db.limit becomes a pass-through.
    //
    // The lowered nl (see sort_topk.nl.mlir) has no nl.limit at all - the
    // accumulator keeps only the best 2 rows (O(k) memory, O(rows log k) sort)
    // and the scan loop stays unbounded, since top-K must still see every row.
    %a = db.scan_nodes()

    %sa = db.sort(%a) keys [0] ascending [false]

    %la = db.limit(%sa) count 2

    db.output(%la)

    return
  }
}
