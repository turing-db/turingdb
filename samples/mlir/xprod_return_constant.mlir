func.func @main() {
  %0:2 = db.cross_product factor {
    %a = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %a : !db.column<!storage.node_id>
  } factor {
    %b = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %b : !db.column<!storage.node_id>
  }
  %c = db.constant(5 : i64)
  %rows = db.broadcast_constant(%c, %0#0) : (!db.column<i64>, !db.column<!storage.node_id>) -> !db.column<i64>
  db.output(%rows) : !db.column<i64>
  return
}
