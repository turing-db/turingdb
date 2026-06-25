module {
  func.func @main() {
    // MATCH (a) RETURN a LIMIT 3: a scan capped to its first three rows.
    //
    // db.limit lowers to a hoisted nl.limit handle, a `limit` operand on the
    // scan loop (so it stops once the budget is spent), an nl.limit_update that
    // charges each chunk against the budget, and an nl.limit_truncate that cuts
    // the chunk to the prefix the budget allows before nl.output emits it.
    %a = db.scan_nodes() : !db.column<"a">

    %la = db.limit(%a) count 3 : (!db.column<"a">) -> !db.column<"a">

    db.output(%la) : !db.column<"a">

    return
  }
}
