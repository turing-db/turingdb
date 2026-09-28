// MATCH (n:Person{name:'Cyrus'}) WHERE n.age = 23 RETURN n
func.func @main() {
  %0 = nl.constant(23)
  %1 = nl.get_property_type("age")
  %2 = nl.constant("Cyrus")
  %3 = nl.get_property_type("name")
  %4 = nl.scan_nodes()
  nl.for %arg0 in %4 {
    %5 = nl.get_node_properties(%arg0, %3)
    %6 = nl.eq %5, %2
    %7 = nl.filter %6, (%arg0)
    %8 = nl.get_node_label_set(%7)
    %9 = nl.check_label_constraint(%8, ["Person"])
    %10 = nl.filter %9, (%7)
    %11 = nl.get_node_properties(%10, %1)
    %12 = nl.eq %11, %0
    %13 = nl.filter %12, (%10)
    nl.output(%13)
  }
  return
}

