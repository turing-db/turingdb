// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }
func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  %1 = db.get_node_properties(%0, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
  %2 = db.count_subquery(%0) {
  ^bb0(%arg0: !db.column<!storage.node_id>):
    %3 = db.row_constant %arg0 : !db.column<!storage.node_id>
    %4 = db.distinct_set : !db.distinct_set
    %5 = db.union {
      %6, %7, %8, %9 = db.get_out_edges_by_type(%3, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      %10 = db.get_node_properties(%9, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
      %11 = db.remove_duplicates(%10) against %4 : (!db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.nullable<!storage.string>>
      db.yield %11 : !db.column<!storage.nullable<!storage.string>>
    }, {
      %6, %7, %8, %9 = db.get_out_edges_by_type(%3, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
      %10 = db.get_node_properties(%9, "name") : (!db.column<!storage.node_id>) -> !db.column<!storage.nullable<!storage.string>>
      %11 = db.remove_duplicates(%10) against %4 : (!db.column<!storage.nullable<!storage.string>>) -> !db.column<!storage.nullable<!storage.string>>
      db.yield %11 : !db.column<!storage.nullable<!storage.string>>
    } : !db.column<!storage.nullable<!storage.string>>
    db.count_subquery_yield {%5} : {!db.column<!storage.nullable<!storage.string>>}
  } : (!db.column<!storage.node_id>) -> !db.column<ui64>
  db.output(%1, %2) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }"] : !db.column<!storage.nullable<!storage.string>>, !db.column<ui64>
  return
}
