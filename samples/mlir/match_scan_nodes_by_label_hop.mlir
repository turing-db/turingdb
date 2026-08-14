// MATCH (n:Person:SoftwareEngineering)-->() RETURN  *

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person", "SoftwareEngineering"]) : !db.column<!storage.node_id>
  %1, %2, %3, %4 = db.get_out_edges(%0, {}) : (!db.column<!storage.node_id>) -> (!db.column<!storage.node_id>, !db.column<!storage.edge_id>, !db.column<!storage.edge_type_id>, !db.column<!storage.node_id>)
  db.output(%1) names ["n"] : !db.column<!storage.node_id>
  return
}
