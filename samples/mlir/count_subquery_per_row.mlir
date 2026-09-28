// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.count_subquery(%0) {
  ^bb0(%arg0: !db.column<!storage.node_id>):
    %3, %4, %5, %6 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %7 = db.limit(%6) count 2 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.count_subquery_yield {%7} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%1, %2) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }"] : !db.column<none>, !db.column<ui64>
  return
}
