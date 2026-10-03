// UNWIND [0, 6, 99] AS x MATCH (n)-[e:INTERESTED_IN]->(m) WHERE n = x RETURN n.name, m.name
func.func @main() {
  %0 = db.const_scan_nodes([0, 6, 99]) : !db.column<!storage.node_id>
  %1, %2, %3, %4 = db.get_out_edges_by_type(%0, ["INTERESTED_IN"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %5 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  %6 = db.get_node_properties(%4, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%5, %6) names ["n.name", "m.name"] : !db.column<none>, !db.column<none>
  return
}
