module {
  func.func @main() {
    // MATCH (a) RETURN a.team, min(a.score), max(a.score): scan every node, read
    // its "team" and "score", group the nodes by team and reduce each group's
    // scores two ways at once - a min and a max. keys 1 marks the leading column
    // (team) as the grouping key; aggregates [min, max] names the two reductions
    // over the trailing columns, so the op takes one key column and two
    // aggregate-input columns (both the score column) and emits one row per team:
    // the team, its lowest score, and its highest score.
    //
    // Like a bare aggregate, Cypher aggregates ignore nulls within a group; min and
    // max keep the input's value type (unlike avg, which widens to a float). The db
    // result value types are resolved during the db -> nl lowering (hence -graph),
    // so they are left none here - both min and max keep score's Int64.
    //
    // min/max reduce the values themselves, so - unlike count(*) - the input must be
    // a property value column, not an ID column; the reduction needs an orderable
    // column (numbers, strings, bools), so an embedding min is rejected at lowering.
    //
    // db.group_aggregate is a pipeline breaker like db.sort: it lowers to a hoisted
    // nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update inside the
    // scan loop that folds each chunk into the per-group state, and - after the loop
    // - an nl.group_aggregate source iterator whose nl.for emits one row per group.
    //
    // "team" and "score" are placeholders - swap them for properties your graph has;
    // min/max need an orderable column.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>

    %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gteam, %lo, %hi = db.group_aggregate(%team, %score, %score) keys 1 aggregates [min, max] : (!db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>, !db.column<none>)

    db.output(%gteam, %lo, %hi) : !db.column<none>, !db.column<none>, !db.column<none>

    return
  }
}
