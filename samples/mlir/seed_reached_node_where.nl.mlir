// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (a)-->(n) WHERE n = ids AND a.age > 20 RETURN a.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.constant(20 : i64)
  %2 = nl.get_property_type("age")
  %3 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %4 = nl.get_in_edges(%nodes, {})
    nl.for %arg2, %arg3, %arg4, %arg5 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %5 = nl.get_node_properties(%arg2, %2) : !nl.chunk<!storage.nullable<i64>>
      %6 = nl.gt %5, %1 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
      %7 = nl.filter %6, (%arg2) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
      %8 = nl.get_node_properties(%7, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%8) names ["a.name"] : !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
