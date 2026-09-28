// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.count_subquery(%0) {
  ^bb0(%arg0: !db.column<!storage.node_id>):
    %3 = db.distinct_set : !db.distinct_set
    %4 = db.union {
      %5, %6, %7, %8 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      %9 = db.get_node_properties(%8, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
      %10 = db.remove_duplicates(%9) against %3 : (!db.column<none>) -> !db.column<none>
      db.yield %10 : !db.column<none>
    }, {
      %5, %6, %7, %8 = db.get_out_edges_by_type(%arg0, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      %9 = db.get_node_properties(%8, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
      %10 = db.remove_duplicates(%9) against %3 : (!db.column<none>) -> !db.column<none>
      db.yield %10 : !db.column<none>
    } : !db.column<none>
    db.count_subquery_yield {%4} : {!db.column<none>}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%1, %2) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }"] : !db.column<none>, !db.column<ui64>
  return
}
