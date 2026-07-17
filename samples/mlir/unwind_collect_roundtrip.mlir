module {
  func.func @main() {
    // MATCH (a) WITH collect(a.name) AS names UNWIND names AS name RETURN name: the
    // canonical collect -> unwind round trip. An ungrouped collect gathers every node's
    // name into one list, and UNWIND immediately expands it back into one row per name -
    // so the projection ends the same shape it started, minus the correlation to `a`
    // (collect is an aggregation horizon). The list never materializes.
    //
    // keys 0 is the ungrouped form: one global group, re-emitted element by element.
    // Cypher collect drops nulls, so a node with no "name" contributes no row. The
    // unwound value type is resolved during lowering (hence -graph); name is a String.
    //
    // db.unwind_collect lowers to a hoisted nl.collect_buffer, an nl.collect_update
    // inside the scan loop, and an nl.unwind_collect source draining the single group's
    // elements after the loop.
    //
    // "name" is a simpledb property; swap it for a property your graph has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %name = db.get_node_properties(%a, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    %uname = db.unwind_collect(%name) keys 0 : (!db.column<none>) -> !db.column<none>

    db.output(%uname) : !db.column<none>

    return
  }
}
