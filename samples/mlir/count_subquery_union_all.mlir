// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.count_subquery(%0) carries_scope {
  ^bb0(%arg0: !db.column<!storage.node_id>, %arg1: !db.column<ui64>):
    %5, %6, %7, %8, %9 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {%arg1}) : (!db.column<!storage.node_id>, !db.column<ui64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<ui64>)
    db.count_subquery_yield %9, {}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  %3 = db.count_subquery(%0) carries_scope {
  ^bb0(%arg0: !db.column<!storage.node_id>, %arg1: !db.column<ui64>):
    %5, %6, %7, %8, %9 = db.get_out_edges_by_type(%arg0, ["KNOWS_WELL"], {%arg1}) : (!db.column<!storage.node_id>, !db.column<ui64>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>, !db.column<ui64>)
    db.count_subquery_yield %9, {}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  %4 = db.add %2, %3 : (!db.column<ui64>, !db.column<ui64>) -> !db.column<none>
  db.output(%1, %4) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }"] : !db.column<none>, !db.column<none>
  return
}
