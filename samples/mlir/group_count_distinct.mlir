module {
  func.func @main() {
    // MATCH (a)-->(b) RETURN a.name, count(DISTINCT b): group each source's out-edge
    // rows by its "name" and count how many different nodes that source points at.
    // count_distinct charges a target once per group however many edges reach it,
    // where count would charge every edge row - so the two kinds disagree exactly
    // when a group repeats a value.
    //
    // count_distinct is its own GroupAggregateKind rather than a modifier on count,
    // so the kinds array stays the whole aggregate spec. It keeps count's shape: the
    // per-group state is the tally alone (no reduced value), and the result is a
    // non-null ui64 per group. The one difference is the fold, which keys each row on
    // its (group, value) pair and charges the group only the first time it sees one.
    //
    // The grouping key's value type is resolved during the db -> nl lowering (hence
    // -graph), so the key chunk element type is baked from the "name" property.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %src, %edge, %type, %tgt = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    %name = db.get_node_properties(%src, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gname, %n = db.group_aggregate(%name, %tgt) keys 1 aggregates [count_distinct] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)

    db.output(%gname, %n) : !db.column<none>, !db.column<ui64>

    return
  }
}
