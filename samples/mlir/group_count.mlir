module {
  func.func @main() {
    // MATCH (a) RETURN a.team, count(*): scan every node, read its "team", and
    // group the nodes by team, counting every node in each group. count(*) counts
    // rows regardless of value, so its input is the never-null node IDs (%a) - the
    // same never-null-column trick db.count uses to express count(*).
    //
    // db.group_aggregate is the grouped counterpart of a bare aggregate. keys 1
    // says the first column (team) is the grouping key; aggregates [0] names one
    // count reduction (kind 0) over the trailing column. It is a pipeline breaker
    // like db.sort - it must see every row before it can emit any - so it lowers to
    // a hoisted nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update
    // inside the scan loop that folds each chunk into the per-group state, and -
    // after the loop - an nl.group_aggregate source iterator whose nl.for emits one
    // row per group. Unlike db.count it does not collapse to one row, so it has an
    // emit loop.
    //
    // The grouping key "team" is a placeholder - swap it for a property your graph
    // has. Its value type is resolved during the db -> nl lowering (hence -graph),
    // so the key chunk element type is baked from it. count is always a ui64.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gteam, %n = db.group_aggregate(%team, %a) keys 1 aggregates [0] : (!db.column<none>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<ui64>)

    db.output(%gteam, %n) : !db.column<none>, !db.column<ui64>

    return
  }
}
