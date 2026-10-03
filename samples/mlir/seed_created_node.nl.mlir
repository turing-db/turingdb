// MATCH (r {name: 'Remy'}) CREATE (z:Person {name: 'Zed'})-[:KNOWS_WELL]->(r) WITH z MATCH (n)-[:KNOWS_WELL]->(m) WHERE n = z RETURN m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["KNOWS_WELL"])
  %2 = nl.constant("Zed" : !storage.string)
  %3 = nl.scan_nodes_by_property_value("name", "Remy" : !storage.string)
  nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.create_node ["Person"], ["name"], {%2} foreach %arg0 : !nl.chunk<!storage.node_id> : !nl.chunk<!storage.string>
    %5 = nl.create_edge %4, %arg0, "KNOWS_WELL", [], {} src_all_pending
    %nodes = nl.fetch_nodes(%4, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %6 = nl.get_out_edges_by_type(%nodes, %1, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %6 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %7 = nl.get_node_properties(%arg4, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%7) names ["m.name"] : !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
