// MATCH (n:Person) RETURN  *

func.func @main() {
  %0 = db.scan_nodes_by_label(["Person"]) : !db.column<!storage.node_id>
  db.output(%0) names ["n"] : !db.column<!storage.node_id>
  return
}
