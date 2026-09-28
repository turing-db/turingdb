// Generated nl-dialect lowering of sample.mlir (db dialect).
// Reproduce with: mlir -dump-lowered sample.mlir
// This is the DBLowering output; edit sample.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.scan_nodes()
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %1 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %1 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %2 = nl.get_out_edges(%arg4, {%arg1}) : !nl.chunk<!storage.node_id>
        nl.for %arg5, %arg6, %arg7, %arg8, %arg9 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>> {
          nl.output(%arg9, %arg5, %arg8) : !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>, !nl.chunk<!storage.node_id>
        }
      }
    }
    return
  }
}
