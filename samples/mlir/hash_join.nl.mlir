// MATCH (n), (m) WHERE n.name = m.name RETURN n, m

func.func @main() {
  %0 = nl.hash_join_buffer build_key 1 probe_key 1
  %1 = nl.get_property_type("name")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<!storage.string>>
    nl.hash_join_collect %0, (%arg0, %4) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>
  }
  %3 = nl.scan_nodes()
  nl.for %arg1 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %4 = nl.get_node_properties(%arg1, %1) : !nl.chunk<!storage.nullable<!storage.string>>
    %5:4 = nl.hash_join_probe %0, (%arg1, %4) : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>) -> (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.nullable<!storage.string>>)
    nl.output(%5#0, %5#2) names ["n", "m"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
  }
  return
}
