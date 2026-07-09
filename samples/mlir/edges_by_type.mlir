module {
  func.func @main() {
    // MATCH (a)-[:KNOWS_WELL]->(b): scan `a`, then one out-edge hop that keeps
    // only the KNOWS_WELL edges. get_out_edges_by_type is the plain get_out_edges
    // narrowed by the relationship type spelled in the query - an edge carries
    // exactly one type, so the filter is an equality on that type.
    //
    // The type name is resolved against the loaded graph's schema (-graph) at
    // execution, the same way scan_nodes_by_label resolves its labels. A name no
    // edge was ever created with matches nothing, so the hop yields no rows. Swap
    // "KNOWS_WELL" for an edge type your graph actually has.
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %s, %e, %et, %b = db.get_out_edges_by_type(%a, "KNOWS_WELL", {})
        : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    // Project the (a, b) pairs linked by a KNOWS_WELL edge.
    db.output(%s, %b) : !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
