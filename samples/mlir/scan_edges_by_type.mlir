module {
  func.func @main() {
    // Scan every KNOWS_WELL edge in the graph:
    //
    //   MATCH (a)-[:KNOWS_WELL]->(b) RETURN a, b
    //
    // db.scan_edges_by_type opens the dataflow with the edges of one type -
    // source node, edge, edge type and target node - the same four columns
    // db.scan_edges exposes, narrowed to the named type. The whole-graph node
    // scan and its typed hop that this query is written as compile to exactly
    // this op: fuse_scan_edges folds the scan and hop into a db.scan_edges, then
    // fuse_scan_edges_by_type folds the edge-type check into the scan.
    %s, %e, %et, %t = db.scan_edges_by_type("KNOWS_WELL") : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>

    db.output(%s, %t) : !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
