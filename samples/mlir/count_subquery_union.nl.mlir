// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }
func.func @main() {
  %0 = nl.get_edge_type_set(["KNOWS_WELL"])
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.get_property_type("name")
  %3 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
    %5 = nl.each_row{%arg0, %4} : {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>}
    nl.for %arg1, %arg2 in %5 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>> {
      %6 = nl.row_buffer
      %7 = nl.distinct
      %8 = nl.row_constant %arg1 : <!storage.node_id>
      %state, %tag = nl.count_subquery_buffer(%arg1) : {!nl.chunk<!storage.node_id>}
      %9 = nl.broadcast_constant %8, %arg1 : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
      %10 = nl.get_out_edges_by_type(%9, %1, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %10 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %15 = nl.get_node_properties(%arg6, %2) : !nl.chunk<!storage.nullable<!storage.string>>
        %16 = nl.distinct_filter %7, (%15) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.row_collect %6, (%16) : !nl.chunk<!storage.nullable<!storage.string>>
      }
      %11 = nl.broadcast_constant %8, %arg1 : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
      %12 = nl.get_out_edges_by_type(%11, %0, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %12 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %15 = nl.get_node_properties(%arg6, %2) : !nl.chunk<!storage.nullable<!storage.string>>
        %16 = nl.distinct_filter %7, (%15) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.row_collect %6, (%16) : !nl.chunk<!storage.nullable<!storage.string>>
      }
      %13 = nl.row_drain(%6) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>>
      nl.for %arg3 in %13 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>> {
        nl.count_subquery_tally %state rows(%arg3 : !nl.chunk<!storage.nullable<!storage.string>>)
      }
      %14 = nl.count_subquery_result(%state, %arg1) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
      nl.output(%arg2, %14) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>
    }
  }
  return
}
