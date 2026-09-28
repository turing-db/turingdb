// MATCH (n:Person) CALL db.labels() YIELD label RETURN n.name, label

func.func @main() {
  %0 = nl.get_property_type("name")
  %1 = nl.procedure("db.labels") yields ["label"]
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 {
    %3 = nl.procedure_init(%1, (), {})
    nl.for %arg1 in %3 {
      %4 = nl.cross_product{%arg0} {%arg1}
      nl.for %arg2, %arg3 in %4 {
        %5 = nl.get_node_properties(%arg2, %0)
        nl.output(%5, %arg3) names ["n.name", "label"]
      }
    }
  }
  return
}
