// MATCH (n)-->()-->() WHERE n.age = 32 RETURN *

func.func @main() {
  %0 = nl.constant(32 : i64)
  %1 = nl.get_property_type("age")
  %2 = nl.scan_nodes()
  nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
    %3 = nl.get_node_properties(%arg0, %1) : !nl.chunk<!storage.nullable<i64>>
    %4 = nl.eq %3, %0 : (!nl.chunk<!storage.nullable<i64>>, !nl.chunk<i64>) -> !nl.chunk<!storage.nullable<i1>>
    %5 = nl.filter %4, (%arg0) : (!nl.chunk<!storage.nullable<i1>>, !nl.chunk<!storage.node_id>) -> !nl.chunk<!storage.node_id>
    %6 = nl.get_out_edges(%5, {})
    nl.for %arg1, %arg2, %arg3, %arg4 in %6 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
      %7 = nl.get_out_edges(%arg4, {%arg1, %arg2, %arg3}) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>
      nl.for %arg5, %arg6, %arg7, %arg8, %arg9, %arg10, %arg11 in %7 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>> {
        nl.output(%arg9) names ["n"] : !nl.chunk<!storage.node_id>
      }
    }
  }
  return
}
