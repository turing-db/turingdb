// MATCH (n:Person{name:'Cyrus'}) WHERE n.age = 23 RETURN n
func.func @main() {
  %0 = nl.constant(23 : i64)
  %1 = nl.get_property_type("age")
  %2 = nl.constant("Cyrus" : !storage.string)
  %3 = nl.get_property_type("name")
  %4 = nl.scan_nodes()
  nl.for %arg0 in %4 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %5 = nl.get_node_properties(%arg0, %3) : !nl.chunk<!storage.nullable<!storage.string>>
    %6 = nl.eq %5, %2 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %7 = nl.filter %6, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %8 = nl.get_node_label_set(%7) : !nl.chunk<!storage.labelset_id>
    %9 = nl.check_label_constraint(%8, [0, 1, 6, 7, 9]) : !nl.chunk<!storage.bool>
    %10 = nl.filter %9, (%7) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %11 = nl.get_node_properties(%10, %1) : !nl.chunk<!storage.nullable<i64>>
    %12 = nl.eq %11, %0 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %13 = nl.filter %12, (%10) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%13) : !nl.chunk<!storage.node_id>
  }
  return
}

