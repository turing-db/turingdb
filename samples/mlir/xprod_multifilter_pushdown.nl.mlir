// MATCH (n), (m) WHERE n.age = 32 and n.name = 'Remy' AND m.age IS NULL RETURN *

func.func @main() {
  %0 = nl.constant("" : !storage.nullable<none>)
  %1 = nl.constant("Remy" : !storage.string)
  %2 = nl.get_property_type("name")
  %3 = nl.constant(32 : i64)
  %4 = nl.get_property_type("age")
  %5 = nl.scan_nodes()
  nl.for %arg0 in %5 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %6 = nl.get_node_properties(%arg0, %4) : !nl.chunk<!storage.nullable<i64>>
    %7 = nl.eq %6, %3 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %8 = nl.filter %7, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %9 = nl.get_node_properties(%8, %2) : !nl.chunk<!storage.nullable<!storage.string>>
    %10 = nl.eq %9, %1 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %11 = nl.filter %10, (%8) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %12 = nl.scan_nodes()
    nl.for %arg1 in %12 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %13 = nl.get_node_properties(%arg1, %4) : !nl.chunk<!storage.nullable<i64>>
      %14 = nl.eq %13, %0 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<!storage.nullable<none>>) -> !nl.chunk<!storage.nullable<i1>>
      %15 = nl.filter %14, (%arg1) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
      %16:2 = nl.cross_product{%11} {%15} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
      nl.output(%16#0, %16#1) names ["n", "m"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
    }
  }
  return
}
