// MATCH (n:Person{name:'Cyrus'}) RETURN n
func.func @main() {
  %0 = nl.constant("Cyrus")
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 {
    %3 = nl.get_node_properties(%arg0, %1)
    %4 = nl.eq %3, %0
    %5 = nl.filter %4, (%arg0)
    %6 = nl.get_node_label_set(%5)
    %7 = nl.check_label_constraint(%6, ["Person"])
    %8 = nl.filter %7, (%5)
    nl.output(%8)
  }
  return
}

