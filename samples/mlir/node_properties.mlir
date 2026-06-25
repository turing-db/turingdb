module {
  func.func @main() {
    // MATCH (a) RETURN a, a.name: scan every node, then read one property of
    // each into a new column. A node that lacks the property comes back with a
    // null value - none are dropped (the lowering uses the with-null fetch).
    //
    // "name" must be a property that exists in the loaded graph (-graph): the
    // db -> nl lowering resolves the name against the schema and bakes its value
    // type into the result chunk. Swap it for a property your graph actually has.
    %a = db.scan_nodes() : !db.column<none>

    %name = db.get_node_properties(%a, "name") : (!db.column<none>) -> !db.column<none>

    db.output(%a, %name) : !db.column<none>, !db.column<none>

    return
  }
}
