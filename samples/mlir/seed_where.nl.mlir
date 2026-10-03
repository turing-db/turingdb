// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n) WHERE n = ids AND n.name <> 'Adam' RETURN n.name
func.func @main() {
  %0 = nl.constant("Adam" : !storage.string)
  %1 = nl.get_property_type("name")
  %2 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %3 = nl.get_node_properties(%nodes, %1) : !nl.chunk<!storage.nullable<!storage.string>>
    %4 = nl.neq %3, %0 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %5 = nl.filter %4, (%3) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<!storage.string>>
    nl.output(%5) names ["n.name"] : !nl.chunk<!storage.nullable<!storage.string>>
  }
  return
}
