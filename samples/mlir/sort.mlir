module {
  func.func @main() {
    // MATCH (a) RETURN a ORDER BY a DESC: scan every node, then sort the node
    // column by itself, descending.
    //
    // db.sort is a pipeline breaker, so it lowers to a hoisted nl.sort_buffer
    // accumulator, an nl.sort_collect inside the scan loop that appends each
    // chunk, and - after the loop - an nl.sort source iterator whose nl.for emits
    // the accumulated rows in sorted order. `keys [0]` sorts by column 0 (the
    // only column) and `ascending [false]` makes it descending.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %sa = db.sort(%a) keys [0] ascending [false] : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    db.output(%sa) : !db.column<!storage.node_id>

    return
  }
}
