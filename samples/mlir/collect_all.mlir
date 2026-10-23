module {
  func.func @main() {
    // MATCH (a) WITH collect(a.name) AS names RETURN names: an ungrouped collect. With
    // no grouping key (keys 0) every node falls into one group, so the whole scan
    // collapses to a single row holding one list of every node's name. Cypher collect
    // drops nulls, so a node with no "name" contributes nothing.
    //
    // keys 0 is the ungrouped form: the op takes only the collected value column and
    // emits exactly one row - the list. Its element is the collected column's type, a
    // nullable String here.
    //
    // db.collect lowers to a hoisted nl.collect_buffer, an nl.collect_update inside the
    // scan loop, and an nl.collect source draining the single group after the loop.
    //
    // "name" is a simpledb property; swap it for a property your graph has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>

    %names = db.collect(%name) keys 0 : (!db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.list<!storage.nullable<!storage.string>>>

    db.output(%names) : !db.column<!storage.list<!storage.nullable<!storage.string>>>

    return
  }
}
