// MATCH (n)-->()-->() WHERE n.age = 32 RETURN *

func.func @main() {
  %0 = db.scan_nodes() : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "age") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.constant(32 : i64)
  %3 = db.eq %1, %2 : (!db.column<none>, !db.column<i64>) -> !db.column<!storage.bool>
  %4 = db.filter(%3, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %5, %6, %7, %8 = db.get_out_edges(%4, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %9, %10, %11, %12, %13, %14, %15 = db.get_out_edges(%8, {%5, %6, %7}) : (!db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>)
  db.output(%13) names ["n"] : !db.column<!storage.node_id>
  return
}
