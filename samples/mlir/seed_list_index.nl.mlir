// WITH [0, 1, 6, 99] AS ss UNWIND range(0, 3) AS i MATCH (m)-[e]->(n:Person) WHERE n = ss[i] RETURN m.name, n.name
func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.constant([0, 1, 6, 99])
  %2 = nl.constant(0 : i64)
  %3 = nl.constant(3 : i64)
  %4 = nl.broadcast_constant %3 : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  %5 = nl.broadcast_constant %2 : (!nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i64>>
  %6 = nl.range %5, %4 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<!storage.list<i64>>>
  %7 = nl.unwind(%6, {}) : (!nl.chunk<!storage.nullable<!storage.list<i64>>>) -> !nl.iter<!nl.chunk<!storage.nullable<i64>>>
  nl.for %arg0 in %7 : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
    %8 = nl.list_index %1, %arg0 : (!nl.chunk<!storage.list<i64>>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<i64>>
    %nodes = nl.fetch_nodes(%8, {}) : (!nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.node_id>
    %9 = nl.get_node_label_set(%nodes) : !nl.chunk<!storage.labelset_id>
    %10 = nl.check_label_constraint(%9, ["Person"]) : !nl.chunk<!storage.bool>
    %11 = nl.filter %10, (%nodes) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %12 = nl.get_in_edges(%11, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %12 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %13 = nl.get_node_properties(%arg1, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      %14 = nl.get_node_properties(%arg4, %0) : !nl.chunk<!storage.nullable<!storage.string>>
      nl.output(%13, %14) names ["m.name", "n.name"] : !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>
    }
  }
  return
}
