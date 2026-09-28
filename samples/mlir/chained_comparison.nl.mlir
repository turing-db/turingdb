// MATCH (n) RETURN 30 < n.age < 33
func.func @main() {
  %0 = nl.constant(33 : i64)
  %1 = nl.get_property_type("age")
  %2 = nl.constant(30 : i64)
  %3 = nl.scan_nodes()
  nl.for %arg0 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
    %5 = nl.lt %2, %4 : (!nl.chunk<i64>, !nl.chunk<!storage.nullable<i64>>) -> !nl.chunk<!storage.nullable<i1>>
    %6 = nl.lt %4, %0 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %7 = nl.and %5, %6 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
    nl.output(%7) names ["30 < n.age < 33"] : !nl.chunk<!storage.nullable<i1>>
  }
  return
}
