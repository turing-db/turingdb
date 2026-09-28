// MATCH (p:Person) RETURN p.name, EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"])
  %1 = db.get_node_properties(%0, "name")
  %2 = db.exists_subquery(%0) {
  ^bb0(%arg0):
    %3, %4, %5, %6 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {})
    %7 = db.get_node_properties(%6, "name")
    %8 = db.constant("Gym")
    %9 = db.eq %7, %8
    %10 = db.filter(%9, {%6})
    %11 = db.limit(%10) count 1
    db.exists_yield {%11}
  }
  db.output(%1, %2) names ["p.name", "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }"]
  return
}
