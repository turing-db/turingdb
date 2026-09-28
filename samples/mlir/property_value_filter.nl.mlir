// MATCH (n) WHERE n.age = 32 RETURN n, as codegen emits it before the passes run

func.func @main() {
  %0 = nl.constant(32)
  %1 = nl.get_property_type("age")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 {
    %3 = nl.get_node_properties(%arg0, %1)
    %4 = nl.eq %3, %0
    %5 = nl.filter %4, (%arg0)
    nl.output(%5) names ["n"]
  }
  return
}
