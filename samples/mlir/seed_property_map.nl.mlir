// VECTOR SEARCH IN people FOR 3 (1.0, 0.0, 0.0, 0.0) YIELD ids MATCH (n:Person {age: 32}) WHERE n = ids RETURN n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.constant(32 : i64)
  %2 = nl.get_property_type("age")
  %3 = nl.vector_search("people", 3, [1.000000e+00, 0.000000e+00, 0.000000e+00, 0.000000e+00]) : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>>
  nl.for %arg0, %arg1 in %3 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<f64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %4 = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
    %5 = nl.check_label_constraint(%4, ["Person"]) : !nl.chunk<!storage.bool>
    %6 = nl.filter %5, (%nodes) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %7 = nl.get_node_properties(%6, %2) : !nl.chunk<!storage.nullable<i64>>
    %8 = nl.eq %7, %1 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %9 = nl.filter %8, (%6) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %10 = nl.get_node_properties(%9, %0) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.output(%10) names ["n.name"] : !nl.chunk<!storage.nullable<!storage.string>>
  }
  return
}
