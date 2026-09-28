module {
  func.func @main() {
    // MATCH (a) RETURN sum(a.score): scan every node, read its "score", and reduce
    // the non-null values to a single row. Each reduction is its own op - db.sum,
    // db.min, db.max, db.avg - the way count(*) is db.count.
    //
    // Like db.count, they are pipeline breakers: each lowers to a hoisted
    // nl.aggregate accumulator, an nl.aggregate_update inside the scan loop that
    // folds each chunk's non-null values, and - after the loop - an
    // nl.aggregate_result that materializes the single reduced row as a nullable
    // value chunk, which a function-scope nl.output emits. It collapses to one row,
    // so there is no emit loop.
    //
    // Cypher aggregates ignore nulls. sum and min/max keep the input's value type;
    // avg always widens to a float. sum/avg need a numeric column, min/max an
    // orderable one. The property value type is resolved during the db -> nl
    // lowering (hence -graph), so the result chunk element type is baked from it -
    // swap "score" for a property your graph actually has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %score = db.get_node_properties(%a, "score") : (!db.column<!storage.node_id>) -> !db.column<none>

    %total = db.sum(%score) : (!db.column<none>) -> !db.column<none>

    db.output(%total) : !db.column<none>

    return
  }
}
