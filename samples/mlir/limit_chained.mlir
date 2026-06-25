module {
  func.func @main() {
    // MATCH (a) WITH a LIMIT 2 MATCH (a)-->(b) RETURN b: a mid-query (chained)
    // limit. The LIMIT bounds the intermediate `a` cardinality - only the scan
    // (a's producer) carries the budget and halts at two - while `b` then fans
    // out unbounded: each of the two `a`s contributes all of its out-edges.
    //
    // This is the case a terminal-only limit cannot express: db.limit feeds the
    // next db.get_out_edges, not db.output. db.limit lowers to an
    // nl.limit_truncate placed just before that inner traversal, handing it a
    // genuinely cut chunk so the downstream loop stays limit-oblivious.
    %a = db.scan_nodes() : !db.column<"a">

    %la = db.limit(%a) count 2 : (!db.column<"a">) -> !db.column<"a">

    %a1, %e0, %et0, %b = db.get_out_edges(%la, {}) : (!db.column<"a">) -> (!db.column<"a1">, !db.column<"e0">, !db.column<"et0">, !db.column<"b">)

    db.output(%b) : !db.column<"b">

    return
  }
}
