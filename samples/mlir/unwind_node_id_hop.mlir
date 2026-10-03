// UNWIND [0, 1] AS x MATCH (c:Person)<-[r:KNOWS_WELL]-(b) WHERE c = x RETURN x, b

func.func @main() {
  %0 = db.const_scan_nodes([0, 1])
  %1 = db.get_node_label_set(%0)
  %2 = db.check_label_constraint(%1, ["Person"])
  %3 = db.filter(%2, {%0})
  %4, %5, %6, %7 = db.get_in_edges_by_type(%3, ["KNOWS_WELL"], {})
  %8 = db.element_id(%7)
  db.output(%8, %4) names ["x", "b"]
  return
}
