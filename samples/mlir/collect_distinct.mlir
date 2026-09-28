module {
  func.func @main() {
    // MATCH (a)-->(b) WITH a.name AS name, collect(DISTINCT b) AS targets RETURN name,
    // targets: group each source's out-edge rows by its "name" and gather the different
    // nodes that source points at. `distinct` collects a value a group has already
    // collected once, so a node reached over several edges is one element - where the
    // plain form appends one element per edge row. It is Cypher's collect(DISTINCT x),
    // and the sibling of the count_distinct kind db.group_aggregate takes: same
    // dedupe, one gathering the values and the other tallying them. The two forms part
    // exactly when a group repeats a value, which simpledb - where no source reaches one
    // target twice - does not.
    //
    // As on db.count the flag stays on the one op rather than becoming a kind of its
    // own: the collection is the same over a deduplicated column. What changes is the
    // fold, which keys each row on its (group, value) pair and appends only the first
    // time it sees one - so the list holds the group's distinct values in first-seen
    // order. The dedupe tally lives in the hoisted accumulator, so it spans the chunks
    // the producing loop steps through.
    //
    // The collected column here is the target node ID, so the list is a
    // !storage.list<!storage.node_id>: the element type is the collected column's,
    // whatever that is. Only the grouping key is resolved during the db -> nl lowering
    // (hence -graph), baked from the "name" property.
    //
    // "name" is a simpledb property; swap it for a property your graph has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %src, %edge, %type, %tgt = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    %name = db.get_node_properties(%src, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gname, %targets = db.collect(%name, %tgt) keys 1 distinct [0] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.list<!storage.node_id>>)

    db.output(%gname, %targets) : !db.column<none>, !db.column<!storage.list<!storage.node_id>>

    return
  }
}
