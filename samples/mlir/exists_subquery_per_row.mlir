// MATCH (p:Person) RETURN p.name, EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %2 = db.exists_subquery(%0) {
  ^bb0(%arg0: !db.column<!storage.node_id>):
    %3 = db.row_constant %arg0 : !db.column<!storage.node_id>
    %4, %5, %6, %7 = db.get_out_edges_by_type(%3, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
    %8 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
    %9 = db.constant("Gym" : !storage.string)
    %10 = db.eq %8, %9 : (!db.column<!storage.nullable<!storage.string>>, !db.column<!storage.string>) -> !db.column<!storage.bool>
    %11 = db.filter(%10, {%7}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    %12 = db.limit(%11) count 1 : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
    db.exists_yield {%12} : {!db.column<!storage.node_id>}
  } : (!db.column<!storage.node_id>) -> !db.column<!storage.bool>
  db.output(%1, %2) names ["p.name", "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }"] : !db.column<!storage.nullable<!storage.string>>, !db.column<!storage.bool>
  return
}
