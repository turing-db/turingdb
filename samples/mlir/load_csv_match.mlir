// LOAD CSV 'people.csv' AS row MATCH (n {name: row[0]}) RETURN n.name, row[2]

module {
  func.func @main() {
    // A load reads no column of the graph, so its records have nothing to do with the
    // nodes the pattern matched: the two are crossed, and the predicate the pattern
    // carries cuts the pairs. That is the join a file against a graph is - written as a
    // product and a filter, which is the straightforward emission; turning it into a
    // lookup on the property index is an optimisation pass's job, not codegen's.
    //
    // The load is the right factor, so it is re-read once per chunk of the left one - see
    // the nested loops in load_csv_match.nl.mlir.
    %0:3 = db.cross_product factor {
      %5 = db.scan_nodes() : !db.column<!storage.node_id>
      db.yield %5 : !db.column<!storage.node_id>
    } factor {
      %5:2 = db.load_csv("people.csv", [0 : ui64, 2 : ui64]) : !db.column<!storage.string>, !db.column<!storage.string>
      db.yield %5#0, %5#1 : !db.column<!storage.string>, !db.column<!storage.string>
    }

    // The pattern's {name: row[0]} constraint: a property of the node against a field of
    // the record.
    %1 = db.get_node_properties(%0#0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    %2 = db.eq %1, %0#1 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>

    // Every column in flight goes through the filter - the node and both fields - so they
    // stay row-aligned with each other.
    %3:3 = db.filter(%2, {%0#0, %0#1, %0#2}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.string>, !db.column<!storage.string>) -> (!db.column<!storage.node_id>, !db.column<!storage.string>, !db.column<!storage.string>)

    %4 = db.get_node_properties(%3#0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>

    db.output(%4, %3#2) names ["n.name", "row[2]"] : !db.column<none>, !db.column<!storage.string>

    return
  }
}
