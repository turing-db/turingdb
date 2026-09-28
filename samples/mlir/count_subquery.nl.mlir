// MATCH (p:Person) RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() }
func.func @main() {
  %0 = nl.get_edge_type_set(["INTERESTED_IN"])
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
    %state, %tag = nl.count_subquery_buffer(%arg0) : {!nl.chunk<!storage.node_id>}
    %4 = nl.get_out_edges_by_type(%arg0, %0, {%tag}) : !nl.chunk<ui64>
    nl.for %arg1, %arg2, %arg3, %arg4, %arg5 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<ui64>> {
      nl.count_subquery_tally %state, %arg5
    }
    %5 = nl.count_subquery_result(%state, %arg0) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
    nl.output(%3, %5) names ["p.name", "COUNT { (p)-[:INTERESTED_IN]->() }"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>
  }
  return
}
