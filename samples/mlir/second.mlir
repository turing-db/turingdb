module {
  func.func @second() {
    %0 = db.scan_nodes() : !db.column<!storage.node_id>
    return
  }
}
