module {
  func.func @main() {
    // MATCH (a)-[]->(b) WITH DISTINCT a MATCH (a)-[]->(c) RETURN c: a mid-query
    // (chained) DISTINCT. The first hop's sources `a` repeat once per out-edge;
    // WITH DISTINCT collapses them to the unique source nodes, and the second hop
    // then fans out from each unique `a` exactly once - fewer driver rows than the
    // raw (duplicated) source column would give.
    //
    // Like the chained LIMIT, db.remove_duplicates feeds the next db.get_out_edges,
    // not db.output: nl.distinct_filter hands the traversal a genuinely deduped
    // chunk, so the downstream loop stays DISTINCT-oblivious.
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    %da = db.remove_duplicates(%srcs) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>

    %a2, %e1, %et1, %c = db.get_out_edges(%da, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    db.output(%c) : !db.column<!storage.node_id>

    return
  }
}
