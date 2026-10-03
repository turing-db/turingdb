// MATCH (s:Founder) WITH s MATCH (m)-[e:KNOWS_WELL]->(n) WHERE n = s RETURN m.name
func.func @main() {
  %0 = db.scan_nodes_by_label(["Founder"]) : !db.column<!storage.node_id>
  %nodes = db.fetch_nodes(%0, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %1, %2, %3, %4 = db.get_in_edges_by_type(%nodes, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %5 = db.get_node_properties(%1, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%5) names ["m.name"] : !db.column<none>
  return
}
