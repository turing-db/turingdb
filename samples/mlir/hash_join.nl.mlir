// MATCH (n), (m) WHERE n.name = m.name RETURN n, m

func.func @main() {
  %0 = nl.hash_join_buffer build_key 1 probe_key 1
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 {
    %4 = nl.get_node_properties(%arg0, %1)
    nl.hash_join_collect %0, (%arg0, %4)
  }
  %3 = nl.scan_nodes()
  nl.for %arg1 in %3 {
    %4 = nl.get_node_properties(%arg1, %1)
    %5 = nl.hash_join_probe %0, (%arg1, %4)
    nl.for %arg2, %arg3, %arg4, %arg5 in %5 {
      nl.output(%arg2, %arg4) names ["n", "m"]
    }
  }
  return
}
