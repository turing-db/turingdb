// MATCH (p:Person) RETURN p.name, COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }
func.func @main() {
  %0 = nl.get_edge_type_set(["KNOWS_WELL"])
  %1 = nl.get_edge_type_set(["INTERESTED_IN"])
  %2 = nl.get_property_type("name")
  %3 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %3 {
    %4 = nl.get_node_properties(%arg0, %2)
    %state, %tag = nl.count_subquery_buffer(%arg0)
    %5 = nl.get_out_edges_by_type(%arg0, %1, {%tag})
    nl.for %arg1, %arg2, %arg3, %arg4, %arg5 in %5 {
      nl.count_subquery_tally %state, %arg5
    }
    %6 = nl.count_subquery_result(%state)
    %state_0, %tag_1 = nl.count_subquery_buffer(%arg0)
    %7 = nl.get_out_edges_by_type(%arg0, %0, {%tag_1})
    nl.for %arg1, %arg2, %arg3, %arg4, %arg5 in %7 {
      nl.count_subquery_tally %state_0, %arg5
    }
    %8 = nl.count_subquery_result(%state_0)
    %9 = nl.add %6, %8
    nl.output(%4, %9) names ["p.name", "COUNT { MATCH (p)-[:INTERESTED_IN]->() UNION ALL MATCH (p)-[:KNOWS_WELL]->() }"]
  }
  return
}
