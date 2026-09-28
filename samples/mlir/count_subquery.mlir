// MATCH (p:Person) RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  %1 = db.get_node_properties(%0, "name")
  %2 = db.count_subquery(%0) carries_scope {
  ^bb0(%arg0, %arg1):
    %3, %4, %5, %6, %7 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {%arg1})
    db.count_subquery_yield %7, {%3, %4, %6}
  }
  db.output(%1, %2) names ["p.name", "COUNT { (p)-[:INTERESTED_IN]->() }"]
  return
}
