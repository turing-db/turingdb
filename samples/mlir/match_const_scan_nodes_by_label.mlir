// MATCH (n:Person) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = db.const_scan_nodes([0, 2]) : !db.column<!storage.node_id>
  %1 = db.get_node_label_set(%0) : (!db.column<!storage.node_id>) -> !db.column<!storage.labelset_id>
  %2 = db.check_label_constraint(%1, ["Person"]) : (!db.column<!storage.labelset_id>) -> !db.column<!storage.bool>
  %3 = db.filter(%2, {%0}) : (!db.column<!storage.bool>, !db.column<!storage.node_id>) -> !db.column<!storage.node_id>
  db.output(%3) names ["n"] : !db.column<!storage.node_id>
  return
}
