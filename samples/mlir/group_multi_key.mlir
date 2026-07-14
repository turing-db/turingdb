module {
  func.func @main() {
    // MATCH (a) RETURN a.team, a.city, sum(a.score): scan every node, read its
    // "team", "city" and "score", group the nodes by the (team, city) pair and sum
    // each group's scores. keys 2 marks the two leading columns (team, city) as the
    // grouping keys, so a group is one distinct (team, city) tuple; aggregates [1]
    // names one sum reduction (1 = sum) over the trailing column. The op takes two
    // key columns and one aggregate-input column and emits one row per (team, city)
    // pair: the team, the city, and that pair's score sum.
    //
    // Grouping on more than one key is the only difference from the single-key
    // samples: the group identity is the whole key tuple, and the leading results
    // pass the key columns through in order (team then city). Cypher aggregates
    // ignore nulls within a group; sum keeps the input's value type.
    //
    // The db result value types are resolved during the db -> nl lowering (hence
    // -graph): both keys pass through with their property types and the sum keeps
    // score's Int64, so they are left none here.
    //
    // db.group_aggregate is a pipeline breaker like db.sort: it lowers to a hoisted
    // nl.group_aggregate_buffer accumulator, an nl.group_aggregate_update inside the
    // scan loop that folds each chunk into the per-group state, and - after the loop
    // - an nl.group_aggregate source iterator whose nl.for emits one row per group.
    //
    // "team", "city" and "score" are placeholders - swap them for properties your
    // graph has; sum needs a numeric column.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %team = db.get_node_properties(%a, "team") : (!db.column<!storage.node_id>) -> !db.column<none>

    %city = db.get_node_properties(%a, "city") : (!db.column<!storage.node_id>) -> !db.column<none>

    %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>

    %gteam, %gcity, %total = db.group_aggregate(%team, %city, %score) keys 2 aggregates [1] : (!db.column<none>, !db.column<none>, !db.column<none>) -> (!db.column<none>, !db.column<none>, !db.column<none>)

    db.output(%gteam, %gcity, %total) : !db.column<none>, !db.column<none>, !db.column<none>

    return
  }
}
