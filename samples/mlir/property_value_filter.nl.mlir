// MATCH (n) WHERE n.age = 32 RETURN n, as codegen emits it before the passes run

func.func @main() {
  %0 = nl.constant(32 : i64)
  %1 = nl.get_property_type("age")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
    %4 = nl.eq %3, %0 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %5 = nl.filter %4, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%5) names ["n"] : !nl.chunk<!storage.node_id>
  }
  return
}
