// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD tgt MATCH (tgt)-[:INTERESTED_IN]->(t) RETURN n, t

func.func @main() {
  %0:4 = db.cross_product factor {
    %5 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
    db.yield %5 : !db.column<!storage.node_id>
  } factor {
    %5 = db.scan_nodes() : !db.column<!storage.node_id>
    %6, %7, %8, %9 = db.get_out_edges(%5, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %10 = db.check_edge_type_constraint(%8, ["INTERESTED_IN"]) : (!db.column<!storage.edge_type_id>) -> !db.column<!storage.bool>
    %11:4 = db.filter(%10, {%7, %9, %6, %8}) : (!db.column<!storage.bool>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>) -> (!db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.edge_type_id>)
    db.yield %11#2, %11#0, %11#1 : !db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>
  }
  %1 = db.constant(2 : i64)
  %2:5 = db.call_procedure("gnn.neighbourhoodSample", {%0#0, %1}, {%0#2, %0#3, %0#1, %0#0}) yields ["tgt"] : (!db.column<!storage.node_id>, !db.column<i64>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>) -> (!db.column<none>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>)
  %3 = db.eq %2#3, %2#0 : (!db.column<!storage.node_id>, !db.column<none>) -> !db.column<!storage.bool>
  %4:5 = db.filter(%3, {%2#1, %2#2, %2#3, %2#4, %2#0}) : (!db.column<!storage.bool>, !db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<none>) -> (!db.column<!storage.edge_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<!storage.node_id>, !db.column<none>)
  db.output(%4#3, %4#1) names ["n", "t"] : !db.column<!storage.node_id>, !db.column<!storage.node_id>
  return
}
