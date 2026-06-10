module {
  func.func @main() {
    // MATCH (a)->(b)->(c): scan `a`, then two get_out_edges hops.
    %a = db.scan_nodes() : !db.column<"a">

    // First hop a->b, nothing carried yet so the carry set is `{}`.
    %a1, %e0, %et0, %b = db.get_out_edges(%a, {}) : (!db.column<"a">) -> (!db.column<"a1">, !db.column<"e0">, !db.column<"et0">, !db.column<"b">)

    // Second hop b->c, carrying %a1 so it comes back as %a2 (the `a` that reach a `c`).
    %b2, %e1, %et1, %c, %a2 = db.get_out_edges(%b, {%a1}) : (!db.column<"b">, !db.column<"a1">) -> (!db.column<"b2">, !db.column<"e1">, !db.column<"et1">, !db.column<"c">, !db.column<"a2">)

    return
  }
}
