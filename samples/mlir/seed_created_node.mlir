// MATCH (r {name: 'Remy'}) CREATE (z:Person {name: 'Zed'})-[:KNOWS_WELL]->(r) WITH z MATCH (n)-[:KNOWS_WELL]->(m) WHERE n = z RETURN m.name
func.func @main() {
  %0 = db.constant("Zed" : !storage.string)
  %1 = db.scan_nodes_by_property_value("name", "Remy" : !storage.string) : !db.column<!storage.node_id>
  %2 = db.create_node(["Person"], ["name"], {%0}) foreach %1 : (!db.column<!storage.string>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %3 = db.create_edge(%2, %1, "KNOWS_WELL", [], {}) src_all_pending : (!db.column<!storage.node_id>, !db.column<!storage.node_id>) -> !db.column<!storage.edge_id>
  %nodes = db.fetch_nodes(%2, {}) : (!db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  %4, %5, %6, %7 = db.get_out_edges_by_type(%nodes, ["KNOWS_WELL"], {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  %8 = db.get_node_properties(%7, "name") : (!db.column<!storage.node_id>) -> !db.column<none>
  db.output(%8) names ["m.name"] : !db.column<none>
  return
}
