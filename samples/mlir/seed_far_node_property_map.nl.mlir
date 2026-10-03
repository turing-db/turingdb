// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e:INTERESTED_IN]->(m:Interest {name: 'Ghosts'}) WHERE n = ids RETURN n.name
func.func @main() {
  %0 = nl.constant("Ghosts" : !storage.string)
  %1 = nl.get_property_type("name")
  %2 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %3 = nl.get_out_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %4 = nl.get_node_properties(%arg5, %1) : !nl.chunk<!storage.nullable<!storage.string>>
      %5 = nl.eq %4, %0 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
      %6:3 = nl.filter %5, (%arg2, %arg5, %arg4) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_type_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_type_id>)
      %7 = nl.check_edge_type_constraint(%6#2, ["INTERESTED_IN"]) : !nl.chunk<!storage.bool>
      %8:2 = nl.filter %7, (%6#0, %6#1) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
      %9 = nl.get_node_label_set(%8#1) : !nl.chunk<!storage.labelset_id>
      %10 = nl.check_label_constraint(%9, ["Interest"]) : !nl.chunk<!storage.bool>
      %11 = nl.filter %10, (%8#0) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
      %12 = nl.get_node_properties(%11, %1) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%12) names ["n.name"] : !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
