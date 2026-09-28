// MATCH (n) RETURN n.age, count(n), sum(n.age)

func.func @main() {
  %0 = nl.group_aggregate_buffer keys 1 aggregates [count, sum]
  %1 = nl.get_property_type("age")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 {
    %4 = nl.get_node_properties(%arg0, %1)
    %5 = nl.get_node_properties(%arg0, %1)
    nl.group_aggregate_update %0, (%4, %arg0, %5)
  }
  %3 = nl.group_aggregate(%0)
  nl.for %arg0, %arg1, %arg2 in %3 {
    nl.output(%arg0, %arg1, %arg2)
  }
  return
}
