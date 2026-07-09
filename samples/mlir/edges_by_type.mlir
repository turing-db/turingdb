module {
  func.func @main() {
    %a = db.scan_nodes() : !db.column<!storage.node_id>

    %s, %e, %et, %b = db.get_out_edges_by_type(%a, "KNOWS_WELL", {})
        : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)

    db.output(%s, %b) : !db.column<!storage.node_id>, !db.column<!storage.node_id>

    return
  }
}
