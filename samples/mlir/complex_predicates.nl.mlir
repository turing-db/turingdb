func.func @main() {
  %0 = nl.get_property_type("isFrench")
  %1 = nl.constant(32 : i64)
  %2 = nl.get_property_type("age")
  %3 = nl.constant("Cyrus" : !storage.string)
  %4 = nl.get_property_type("name")
  %5 = nl.scan_nodes()
  nl.for %arg0 in %5 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %6 = nl.scan_nodes()
    nl.for %arg1 in %6 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %7:2 = nl.cross_product{%arg0} {%arg1} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>}
      %8 = nl.get_node_properties(%7#0, %4) : !nl.chunk<!storage.nullable<!storage.string>>
      %9 = nl.eq %8, %3 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
      %10 = nl.get_node_properties(%7#1, %4) : !nl.chunk<!storage.nullable<!storage.string>>
      %11 = nl.get_node_properties(%7#0, %4) : !nl.chunk<!storage.nullable<!storage.string>>
      %12 = nl.eq %10, %11 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.nullable<!storage.string>>) -> !nl.chunk<!storage.nullable<i1>>
      %13 = nl.and %9, %12 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %14 = nl.get_node_properties(%7#1, %2) : !nl.chunk<!storage.nullable<i64>>
      %15 = nl.eq %14, %1 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
      %16 = nl.and %13, %15 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %17:2 = nl.filter %16, (%7#1, %7#0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
      %18 = nl.get_node_properties(%17#1, %0) : !nl.chunk<!storage.nullable<i1>>
      %19 = nl.get_node_properties(%17#0, %0) : !nl.chunk<!storage.nullable<i1>>
      %20 = nl.eq %18, %19 : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.nullable<i1>>) -> !nl.chunk<!storage.nullable<i1>>
      %21:2 = nl.filter %20, (%17#0, %17#1) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
      nl.output(%21#1) : !nl.chunk<!storage.node_id>
    }
  }
  return
}
