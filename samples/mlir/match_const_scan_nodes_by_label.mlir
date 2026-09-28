// MATCH (n:Person) WHERE n = 0 OR n = 2 RETURN n

func.func @main() {
  %0 = db.const_scan_nodes([0, 2])
  %1 = db.get_node_label_set(%0)
  %2 = db.check_label_constraint(%1, ["Person"])
  %3 = db.filter(%2, {%0})
  db.output(%3) names ["n"]
  return
}
