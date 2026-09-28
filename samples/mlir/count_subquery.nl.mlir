// MATCH (p:Person) RETURN p.name, COUNT { (p)-[:INTERESTED_IN]->() }
func.func @main() {
  %0 = nl.get_edge_type_set(["INTERESTED_IN"])
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 {
    %3 = nl.get_node_properties(%arg0, %1)
    %state, %tag = nl.count_subquery_buffer(%arg0)
    %4 = nl.get_out_edges_by_type(%arg0, %0, {%tag})
    nl.for %arg1, %arg2, %arg3, %arg4, %arg5 in %4 {
      nl.count_subquery_tally %state, %arg5
    }
    %5 = nl.count_subquery_result(%state)
    nl.output(%3, %5) names ["p.name", "COUNT { (p)-[:INTERESTED_IN]->() }"]
  }
  return
}
