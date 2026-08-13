// MATCH (n) WHERE n.age = 32 AND n.isFrench = true RETURN *

func.func @main() {
  %0 = nl.constant(true)
  %1 = nl.get_property_type("isFrench")
  %2 = nl.constant(32 : i64)
  %3 = nl.get_property_type("age")
  %4 = nl.scan_nodes()
  nl.for %arg0 in %4 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %5 = nl.get_node_properties(%arg0, %3) : !nl.chunk<!storage.nullable<i64>>
    %6 = nl.eq %5, %2 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %7 = nl.filter %6, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %8 = nl.get_node_properties(%7, %1) : !nl.chunk<!storage.nullable<i1>>
    %9 = nl.eq %8, %0 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<i1>) -> !nl.chunk<!storage.nullable<i1>>
    %10 = nl.filter %9, (%7) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%10) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
