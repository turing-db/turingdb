// MATCH (p:Person) RETURN p.name, EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }
func.func @main() {
  %0 = nl.constant("Gym")
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.get_property_type("name")
  %3 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %3 {
    %4 = nl.get_node_properties(%arg0, %2)
    %5 = nl.each_row{%arg0, %4}
    nl.for %arg1, %arg2 in %5 {
      %6 = nl.limit(1)
      %state, %tag = nl.exists_buffer(%arg1)
      %7 = nl.get_out_edges_by_type(%arg1, %1, {})
      nl.for %arg3, %arg4, %arg5, %arg6 in %7 limit %6 {
        %9 = nl.get_node_properties(%arg6, %2)
        %10 = nl.eq %9, %0
        %11 = nl.filter %10, (%arg6)
        nl.limit_update %6, %11
        %12 = nl.limit_truncate %6, (%11)
        nl.exists_mark %state, (%12)
      }
      %8 = nl.exists_result(%state)
      nl.output(%arg2, %8) names ["p.name", "EXISTS { MATCH (p)-[:INTERESTED_IN]->(i) WHERE i.name = 'Gym' RETURN i LIMIT 1 }"]
    }
  }
  return
}
