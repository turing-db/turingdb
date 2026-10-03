// MATCH (s:Founder) WITH s MATCH (m)-[e:KNOWS_WELL]->(n) WHERE n = s RETURN m.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.get_edge_type_set(["KNOWS_WELL"])
  %2 = nl.scan_nodes_by_label(["Founder"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %3 = nl.get_in_edges_by_type(%nodes, %1, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %4 = nl.get_node_properties(%arg1, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%4) names ["m.name"] : !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
