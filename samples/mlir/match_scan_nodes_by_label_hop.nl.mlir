// MATCH (n:Person:SoftwareEngineering)-->() RETURN  *

func.func @main() {
  %0 = nl.scan_nodes_by_label(["Person", "SoftwareEngineering"])
  nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %1 = nl.get_out_edges(%arg0, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      nl.output(%arg1) names ["n"] : !nl.chunk<!storage.node_id>
    }
  }
  return
}
