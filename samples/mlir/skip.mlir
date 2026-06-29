module {
  func.func @main() {
    // MATCH (a) RETURN a SKIP 3: a scan whose first three rows are dropped.
    //
    // db.skip lowers to a hoisted nl.skip handle, an nl.skip_update that charges
    // each chunk against the remaining rows-to-drop, and an nl.skip_truncate that
    // lifts the surviving suffix of each chunk into a fresh, front-aligned chunk
    // before nl.output emits it. Unlike a limit it threads no operand onto the scan
    // loop (a skip cannot early-exit) and does not fold into nl.output (the suffix
    // must be copied to the front).
    %a = db.scan_nodes() : !db.column<"a">

    %sa = db.skip(%a) count 3 : (!db.column<"a">) -> !db.column<"a">

    db.output(%sa) : !db.column<"a">

    return
  }
}
