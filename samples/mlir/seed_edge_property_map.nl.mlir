// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n)-[e:KNOWS_WELL {duration: 20}]->(m:Person) WHERE n = ids RETURN n.name, m.name, e.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.constant(20 : i64)
  %2 = nl.get_property_type("duration")
  %3 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %4 = nl.get_out_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %5 = nl.get_edge_properties(%arg3, %2) : !nl.chunk<!storage.nullable<i64>>
      %6 = nl.eq %5, %1 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
      %7:4 = nl.filter %6, (%arg2, %arg5, %arg3, %arg4) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>)
      %8 = nl.check_edge_type_constraint(%7#3, ["KNOWS_WELL"]) : !nl.chunk<!storage.bool>
      %9:3 = nl.filter %8, (%7#0, %7#1, %7#2) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>)
      %10 = nl.get_node_label_set(%9#1) : !nl.chunk<!storage.labelset_id>
      %11 = nl.check_label_constraint(%10, ["Person"]) : !nl.chunk<!storage.bool>
      %12:3 = nl.filter %11, (%9#0, %9#1, %9#2) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>)
      %13 = nl.get_node_properties(%12#0, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      %14 = nl.get_node_properties(%12#1, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      %15 = nl.get_edge_properties(%12#2, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%13, %14, %15) names ["n.name", "m.name", "e.name"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
