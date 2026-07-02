module {
  func.func @main() {
    // MATCH (a) RETURN count(a): scan every node and count them. count(a) counts
    // the non-null values of its column; node IDs off a scan are never null, so
    // this is the total node count - the way count(*) is expressed.
    //
    // db.count is a pipeline breaker like db.sort: it lowers to a hoisted nl.count
    // tally, an nl.count_update inside the scan loop that charges each chunk's
    // rows, and - after the loop - an nl.count_result source iterator whose nl.for
    // emits the single tally row (a nullable, always-present i64).
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %count = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<i64>

    db.output(%count) : !db.column<i64>

    return
  }
}
