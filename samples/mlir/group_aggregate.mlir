module {
  func.func @main() {
    // MATCH (a) RETURN a.team, sum(a.score), avg(a.score): scan every node, read
    // its "team" and "score", group the nodes by team and reduce each group's
    // scores two ways at once - a sum and an average. keys 1 marks the leading
    // column (team) as the grouping key; aggregates [sum, avg] names the two
    // reductions over the trailing columns, so the op takes one key column and two
    // aggregate-input columns (both the score column) and emits one row per team:
    // the team, its score sum, and its score average.
    //
    // Like a bare aggregate, Cypher aggregates ignore nulls within a group; sum
    // keeps the input's value type and avg always widens to a float. The db result
    // value types are resolved during the db -> nl lowering (hence -graph), so they
    // are left none here - sum keeps score's Int64, avg becomes an f64.
    //
    // db.group_aggregate is a pipeline breaker like db.sort: it lowers to a hoisted
    // nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update inside the
    // scan loop that folds each chunk into the per-group state, and - after the loop
    // - an nl.group_aggregate source iterator whose nl.for emits one row per group.
    //
    // "team" and "score" are placeholders - swap them for properties your graph has;
    // sum/avg need a numeric column.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>

    %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gteam, %total, %mean = db.group_aggregate(%team, %score, %score) keys 1 aggregates [sum, avg] : (!db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>, !db.column<none>)

    db.output(%gteam, %total, %mean) : !db.column<none>, !db.column<none>, !db.column<none>

    return
  }
}
