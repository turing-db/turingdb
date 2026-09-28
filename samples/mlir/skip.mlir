module {
  func.func @main() {
    // MATCH (a) RETURN a SKIP 3: a scan whose first three rows are dropped.
    //
    // db.skip lowers to a hoisted nl.skip handle, an nl.skip_update that charges
    // each chunk against the remaining rows-to-drop, and an nl.skip_truncate that
    // would lift the surviving suffix of each chunk into a fresh, front-aligned
    // chunk. Unlike a limit it threads no operand onto the scan loop (a skip cannot
    // early-exit). Here the truncate's only consumer is nl.output, so the fold
    // collapses it: nl.output carries the skip handle and emits the surviving
    // suffix in place at an offset (skipThisStep), with no copy at all - see the
    // generated lowering in skip.nl.mlir.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %sa = db.skip(%a) count 3 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    db.output(%sa) : !db.column<!storage.node_id>

    return
  }
}
