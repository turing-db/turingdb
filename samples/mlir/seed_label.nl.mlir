// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n:Person) WHERE n = ids RETURN n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %2 = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
    %3 = nl.check_label_constraint(%2, ["Person"]) : !nl.chunk<!storage.bool>
    %4 = nl.filter %3, (%nodes) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %5 = nl.get_node_properties(%4, %0) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.output(%5) names ["n.name"] : !nl.chunk<!storage.nullable<!storage.string>>
  }
  return
}
