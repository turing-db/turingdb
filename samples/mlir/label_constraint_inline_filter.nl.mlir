// MATCH (n:Person{name:'Cyrus'}) RETURN n
func.func @main() {
  %0 = nl.constant("Cyrus" : !storage.string)
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
    %4 = nl.eq %3, %0 : (!nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.string>) -> !nl.chunk<!storage.nullable<i1>>
    %5 = nl.filter %4, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %6 = nl.get_node_label_set(%5) : !nl.chunk<!storage.labelset_id>
    %7 = nl.check_label_constraint(%6, [0, 1, 6, 7, 9]) : !nl.chunk<!storage.bool>
    %8 = nl.filter %7, (%5) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    nl.output(%8) : !nl.chunk<!storage.node_id>
  }
  return
}

