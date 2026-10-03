// UNWIND [0, 1] AS x MATCH (c:Person)<-[r:KNOWS_WELL]-(b) WHERE c = x RETURN x, b

func.func @main() {
  %0 = nl.get_edge_type_set(["KNOWS_WELL"])
  %1 = nl.const_scan_nodes([0, 1])
  nl.for %arg0 in %1 {
    %2 = nl.get_node_label_set(%arg0)
    %3 = nl.check_label_constraint(%2, ["Person"])
    %4 = nl.filter %3, (%arg0)
    %5 = nl.get_in_edges_by_type(%4, %0, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %5 {
      %6 = nl.element_id %arg4
      nl.output(%6, %arg1) names ["x", "b"]
    }
  }
  return
}
