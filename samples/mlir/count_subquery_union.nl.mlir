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
      %6 = nl.union_buffer
      %7 = nl.distinct
      %state, %tag = nl.count_subquery_buffer(%arg1) : {!nl.chunk<!storage.node_id>}
      %8 = nl.get_out_edges_by_type(%arg1, %1, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %8 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %12 = nl.get_node_properties(%arg6, %2) : !nl.chunk<!storage.nullable<!storage.string>>
        %14 = nl.distinct_filter %7, (%12) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.union_collect %6, (%14) : !nl.chunk<!storage.nullable<!storage.string>>
      }
      %9 = nl.get_out_edges_by_type(%arg1, %0, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %9 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %12 = nl.get_node_properties(%arg6, %2) : !nl.chunk<!storage.nullable<!storage.string>>
        %14 = nl.distinct_filter %7, (%12) : !nl.chunk<!storage.nullable<!storage.string>>
        nl.union_collect %6, (%14) : !nl.chunk<!storage.nullable<!storage.string>>
      }
      %10 = nl.union_drain(%6) : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>>
      nl.for %arg3 in %10 : !nl.iter<!nl.chunk<!storage.nullable<!storage.string>>> {
        nl.count_subquery_tally %state rows(%arg3 : !nl.chunk<!storage.nullable<!storage.string>>)
      }
      %11 = nl.count_subquery_result(%state, %arg1) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<ui64>
      nl.output(%arg2, %11) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->(i) RETURN i.name AS name UNION MATCH (p)-[:KNOWS_WELL]->(k) RETURN k.name AS name }"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<ui64>
    }
  }
  return
}
