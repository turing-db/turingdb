module {
  func.func @main() {
    // MATCH (a) RETURN count(a): scan every node and count them. count(a) counts
    // the non-null values of its column; node IDs off a scan are never null, so
    // this is the total node count - the way count(*) is expressed.
    //
    // db.count is a pipeline breaker like db.sort: it lowers to a hoisted nl.count
    // tally, an nl.count_update inside the scan loop that charges each chunk's
    // rows, and - after the loop - an nl.count_result that materializes the single
    // tally row as an unsigned i64 (!nl.chunk<ui64>), which a function-scope
    // nl.output emits. It collapses to one row, so there is no emit loop.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %count = db.count(%a) : (!db.column<!storage.node_id>) -> !db.column<ui64>

    db.output(%count) : !db.column<ui64>

    return
  }
}
