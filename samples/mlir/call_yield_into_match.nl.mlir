// MATCH (n:Person) CALL gnn.neighbourhoodSample(n, 2) YIELD tgt MATCH (tgt)-[:INTERESTED_IN]->(t) RETURN n, t

func.func @main() {
  %0 = nl.procedure("gnn.neighbourhoodSample") yields ["tgt"]
  %1 = nl.constant(2 : i64)
  %2 = nl.scan_nodes_by_label(["Person"])
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.scan_nodes()
    nl.for %arg1 in %3 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %4 = nl.get_out_edges(%arg1, {})
      nl.for %arg2, %arg3, %arg4, %arg5 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %5 = nl.check_edge_type_constraint(%arg4, [1]) : !nl.chunk<!storage.bool>
        %6:4 = nl.filter %5, (%arg3, %arg5, %arg2, %arg4) : (!nl.chunk<!storage.bool>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_type_id>) -> (!nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_type_id>)
        %7 = nl.cross_product{%arg0} {%6#2, %6#0, %6#1} : {!nl.chunk<!storage.node_id>} {!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>}
        nl.for %arg6, %arg7, %arg8, %arg9 in %7 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>> {
          %8 = nl.procedure_init(%0, (%arg6, %1), {%arg8, %arg9, %arg7, %arg6}) : (!nl.procedure_state, !nl.chunk<!storage.node_id>, !nl.chunk<i64>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>>
          nl.for %arg10, %arg11, %arg12, %arg13, %arg14 in %8 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
            %9 = nl.eq %arg13, %arg10 : (!nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> !nl.chunk<i1>
            %10:5 = nl.filter %9, (%arg11, %arg12, %arg13, %arg14, %arg10) : (!nl.chunk<i1>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>) -> (!nl.chunk<!storage.edge_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>)
            nl.output(%10#3, %10#1) names ["n", "t"] : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
          }
        }
      }
    }
  }
  return
}
