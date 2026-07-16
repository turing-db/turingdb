func.func @main() {
  %0:2 = db.cross_product factor {
    %17 = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %17 : !db.column<!storage.node_id>
  } factor {
    %17 = db.scan_nodes() : !db.column<!storage.node_id>
    db.yield %17 : !db.column<!storage.node_id>
  }
  %1 = db.get_node_properties(%0#0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.constant("Cyrus" : !storage.string)
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
  %4 = db.get_node_properties(%0#1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %5 = db.get_node_properties(%0#0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %6 = db.eq %4, %5 : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %7 = db.and %3, %6 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %8 = db.get_node_properties(%0#1, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %9 = db.constant(32 : i64)
  %10 = db.eq %8, %9 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %11 = db.and %7, %10 : (!db.column<!storage.bool>, !db.column<!storage.bool>) -> !db.column<!storage.bool>
  %12:2 = db.filter(%11, {%0#1, %0#0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %13 = db.get_node_properties(%12#1, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
  %14 = db.get_node_properties(%12#0, "isFrench") : (!db.column<!storage.node_id>) -> !db.column<none>
  %15 = db.eq %13, %14 : (!db.column<none>, !db.column<none>) -> !db.column<!storage.bool>
  %16:2 = db.filter(%15, {%12#0, %12#1}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.node_id>)
  db.output(%16#1) : !db.column<!storage.node_id>
  return
}
