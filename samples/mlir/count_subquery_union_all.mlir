// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  %1 = db.get_node_properties(%0, "name")
  %2 = db.count_subquery(%0) carries_scope {
  ^bb0(%arg0, %arg1):
    %5, %6, %7, %8, %9 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {%arg1})
    db.count_subquery_yield %9, {%5, %6, %8}
  }
  %3 = db.count_subquery(%0) carries_scope {
  ^bb0(%arg0, %arg1):
    %5, %6, %7, %8, %9 = db.get_out_edges_by_type(%arg0, ["KNOWS_WELL"], {%arg1})
    db.count_subquery_yield %9, {%5, %6, %8}
  }
  %4 = db.add %2, %3
  db.output(%1, %4) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }"]
  return
}
