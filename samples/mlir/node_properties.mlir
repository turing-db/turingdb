module {
  func.func @main() {
    // MATCH (a) RETURN a, a.name: scan every node, then read one property of
    // each into a new column. A node that lacks the property comes back with a
    // null value - none are dropped (the lowering uses the with-null fetch).
    //
    // "name" must be a property that exists in the loaded graph (-graph), and the
    // value column is its value type made nullable: a String in simpledb. Swap both
    // for a property your graph actually has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>

    db.output(%a, %name) : !db.column<!storage.node_id>, !db.column<!storage.nullable<!storage.string>>

    return
  }
}
