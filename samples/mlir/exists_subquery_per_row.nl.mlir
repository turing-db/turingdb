// MATCH (p:Person) RETURN p.name, EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }
func.func @main() {
  %0 = nl.constant("Gym" : !storage.string)
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.get_property_type("name")
  %3 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.get_node_properties(%arg0, %2) : !nl.chunk<!storage.nullable<!storage.string>>
    %5 = nl.each_row{%arg0, %4} : {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>}
    nl.for %arg1, %arg2 in %5 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>> {
      %6 = nl.row_constant %arg1 : <!storage.node_id>
      %7 = nl.limit(1)
      %state, %tag = nl.exists_buffer(%arg1) : {!nl.chunk<!storage.node_id>}
      %8 = nl.broadcast_constant %6, %arg1 : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
      %9 = nl.get_out_edges_by_type(%8, %1, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %9 limit %7 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %11 = nl.get_node_properties(%arg6, %2) : !nl.chunk<!storage.nullable<!storage.string>>
        %12 = nl.eq %11, %0 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
        %13 = nl.filter %12, (%arg6) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
        nl.limit_update %7, %13 : !nl.chunk<!storage.node_id>
        %14 = nl.limit_truncate %7, (%13) : !nl.chunk<!storage.node_id>
        nl.exists_mark %state, (%14) : {!nl.chunk<!storage.node_id>}
      }
      %10 = nl.exists_result(%state, %arg1) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.bool>
      nl.output(%arg2, %10) names ["p.name", "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.bool>
    }
  }
  return
}
