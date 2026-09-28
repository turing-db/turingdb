// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }
func.func @main() {
  %0 = nl.get_edge_type_set(["INTERESTED_IN"])
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
    %4 = nl.each_row{%arg0, %3} : {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>}
    nl.for %arg1, %arg2 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>> {
      %5 = nl.limit(2)
      %state, %tag = nl.count_subquery_buffer(%arg1) : {!nl.chunk<!storage.node_id>}
      %6 = nl.get_out_edges_by_type(%arg1, %0, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %6 limit %5 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        nl.limit_update %5, %arg6 : !nl.chunk<!storage.node_id>
        %8 = nl.limit_truncate %5, (%arg6) : !nl.chunk<!storage.node_id>
        nl.count_subquery_tally %state rows(%8 : !nl.chunk<!storage.node_id>)
      }
      %7 = nl.count_subquery_result(%state, %arg1) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
      nl.output(%arg2, %7) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i LIMIT 2 }"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>
    }
  }
  return
}
