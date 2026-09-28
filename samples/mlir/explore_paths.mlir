module {
  func.func @main() {
    // MATCH (n)-[e:KNOWS]->{1,3}(m) RETURN n, e, m, size(e)
    //
    // A variable-length pattern is one op: db.explore_paths enumerates every trail of
    // one to three KNOWS edges leaving each seed, one row per path, and binds the seed,
    // the end node and the path itself - a handle into the query's path trie. `e`
    // expands into its edge list only where a list is consumed, here at the output, and
    // size(e) reads the hop count off the handle.
    %n = db.scan_nodes()

    %0:3 = db.explore_paths(%n, {}) forward hops 1 to 3 edge_types ["KNOWS"]

    %e = db.expand_path(%0#2, %0#0) kind edges
    %l = db.path_length(%0#2)

    db.output(%0#0, %e, %0#1, %l)

    return
  }
}
