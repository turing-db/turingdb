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
    %a = db.scan_nodes()
    %srcs, %eids, %etypes, %b = db.get_out_edges(%a, {})

    %da = db.remove_duplicates(%srcs)

    %a2, %e1, %et1, %c = db.get_out_edges(%da, {})

    db.output(%c)

    return
  }
}
