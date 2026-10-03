// UNWIND [0, 6, 99] AS x MATCH (m)-[e]->(n:Person) WHERE n = x RETURN m.name, n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.unwind_const([0, 6, 99]) : !nl.iter<!nl.chunk<!storage.nullable<i64>>>
  nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
    %nodes = nl.fetch_nodes(%arg0, {}) : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.node_id>
    %2 = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
    %3 = nl.check_label_constraint(%2, ["Person"]) : !nl.chunk<!storage.bool>
    %4 = nl.filter %3, (%nodes) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %5 = nl.get_in_edges(%4, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %5 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %6 = nl.get_node_properties(%arg1, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      %7 = nl.get_node_properties(%arg4, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%6, %7) names ["m.name", "n.name"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
