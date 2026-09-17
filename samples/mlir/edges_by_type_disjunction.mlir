module {
  func.func @main() {
    // Walk the edges of either type out of every node:
    //
    //   MATCH (a)-[:KNOWS_WELL|INTERESTED_IN]->(b) RETURN a, b
    //
    // An edge carries exactly one type, so the several a pattern may name are a
    // disjunction: the hop keeps an edge carrying any one of them. The names ride
    // on the op as an array, and the hop walks them itself rather than leaving a
    // db.check_edge_type_constraint and a db.filter behind it.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %s, %e, %et, %b = db.get_out_edges_by_type(%a, ["KNOWS_WELL", "INTERESTED_IN"], {})
        : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    db.output(%s, %b) : !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
