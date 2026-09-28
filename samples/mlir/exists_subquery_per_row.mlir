// MATCH (p:Person) RETURN p.name, EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %2 = db.exists_subquery(%0) {
  ^bb0(%arg0: !db.column<!storage.node_id>):
    %3, %4, %5, %6 = db.get_out_edges_by_type(%arg0, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %7 = db.get_node_properties(%6, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
    %8 = db.constant("Gym" : !storage.string)
    %9 = db.eq %7, %8 : (!db.column<none>, !db.column<!storage.string>) -> !db.column<!storage.bool>
    %10 = db.filter(%9, {%6}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %11 = db.limit(%10) count 1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.exists_yield {%11} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>
  db.output(%1, %2) names ["p.name", "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }"] : !db.column<none>, !db.column<!storage.bool>
  return
}
