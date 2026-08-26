module {
  func.func @main() {
    // MATCH (a)-[:KNOWS_WELL]->(b), (a)-[e1]->(b), (a)-[e2]->(b) RETURN count(*)
    //
    // Three patterns joining the same pair of nodes. Each one is a hop of its own, so `b`
    // is reached three times over - once per pattern - and the three arrivals have to be
    // collapsed onto one variable. The dependency graph does that with merge edges: every
    // pattern past the first closes a cycle, broken by giving that pattern its own copy of
    // `b` plus a merge edge from the copy to `b`. A node carries at most two of them, so a
    // third copy is cascaded through an intermediate merge node first.
    //
    // A merge lowers to a db.eq over its two sources and a db.filter of every live column,
    // so the cascade reads as a chain of them: the first pairs two copies, the second pairs
    // the third copy against the first's survivor. Past both, all three arrivals are the
    // same node - which is what makes e1 and e2 bind the edge of the pair.
    //
    // The hops run in the order the traversal discovered them, the reverse of the query:
    // e2, then e1, then the anonymous KNOWS_WELL edge.
    %0 = db.scan_nodes() : !db.column<!storage.node_id>

    // Hop 1, (a)-[e2]->(b): nothing carried yet. %4 is e2's copy of `b`.
    %1, %2, %3, %4 = db.get_out_edges(%0, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Hop 2, (a)-[e1]->(b): leaves from the same `a` (%1) and carries hop 1's columns.
    // %8 is e1's copy of `b`, %10 is e2's carried through.
    %5, %6, %7, %8, %9, %10, %11 = db.get_out_edges(%1, {%2, %4, %3}) : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)

    // Hop 3, (a)-[:KNOWS_WELL]->(b): %15 is its copy of `b`, with e2's (%17) and e1's
    // (%19) carried alongside. All three copies are now live in one row.
    %12, %13, %14, %15, %16, %17, %18, %19, %20, %21 = db.get_out_edges(%5, {%9, %10, %6, %8, %7, %11}) : (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>)

    // The KNOWS_WELL constraint, on hop 3's edge type.
    %22 = db.check_edge_type_constraint(%14, ["KNOWS_WELL"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
    %23:10 = db.filter(%22, {%13, %15, %18, %19, %16, %17, %12, %14, %20, %21}) : (!db.column<!storage.bool>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>)

    // First merge: e2's copy of `b` (%23#5) against KNOWS_WELL's (%23#1). Its survivor
    // also stands for the intermediate merge node, so it is carried twice - once as
    // itself, once as that node.
    %24 = db.eq %23#5, %23#1 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
    %25:11 = db.filter(%24, {%23#0, %23#1, %23#2, %23#5, %23#3, %23#4, %23#5, %23#6, %23#7, %23#8, %23#9}) : (!db.column<!storage.bool>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>)

    // Second merge, the root of the cascade: e1's copy (%25#4) against the intermediate
    // node (%25#3). Its survivor is `b`.
    %26 = db.eq %25#4, %25#3 : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.bool>
    %27:12 = db.filter(%26, {%25#4, %25#0, %25#1, %25#2, %25#3, %25#4, %25#5, %25#6, %25#7, %25#8, %25#9, %25#10}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.edge_type_id>)

    // count(*) counts the rows of the query's first variable, `a` (%27#8).
    %28 = db.count(%27#8) : (!db.column<!storage.node_id>) -> !db.column<none>

    db.output(%28) names ["count(*)"] : !db.column<none>

    return
  }
}
