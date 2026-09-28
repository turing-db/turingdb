// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }
func.func @main() {
  %0 = nl.get_edge_type_set(["KNOWS_WELL"])
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.get_property_type("name")
  %3 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
    %state, %tag = nl.count_subquery_buffer(%arg0) : {!nl.chunk<!storage.node_id>}
    %5 = nl.get_out_edges_by_type(%arg0, %1, {%tag}) : !nl.chunk<ui64>
    nl.for %arg1, %arg2, %arg3, %arg4, %arg5 in %5 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<ui64>> {
      nl.count_subquery_tally %state, %arg5
    }
    %6 = nl.count_subquery_result(%state, %arg0) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
    %state_0, %tag_1 = nl.count_subquery_buffer(%arg0) : {!nl.chunk<!storage.node_id>}
    %7 = nl.get_out_edges_by_type(%arg0, %0, {%tag_1}) : !nl.chunk<ui64>
    nl.for %arg1, %arg2, %arg3, %arg4, %arg5 in %7 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<ui64>> {
      nl.count_subquery_tally %state_0, %arg5
    }
    %8 = nl.count_subquery_result(%state_0, %arg0) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
    %9 = nl.add %6, %8 : (!nl.chunk<ui64>, !nl.chunk<ui64>) -> !nl.chunk<i64>
    nl.output(%4, %9) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<i64>
  }
  return
}
