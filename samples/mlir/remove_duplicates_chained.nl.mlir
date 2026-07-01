// Generated nl-dialect lowering of remove_duplicates_chained.mlir (db dialect).
// Reproduce with: mlir -dump-lowered remove_duplicates_chained.mlir
// This is the DBLowering output; edit remove_duplicates_chained.mlir, not this file.
//
// The chained case: nl.distinct_filter emits the deduped sources (%3) and the next
// nl.get_out_edges fans out from that genuinely-cut chunk, nesting a further loop -
// no DISTINCT awareness needed downstream, exactly as a chained nl.limit_truncate.
module {
  func.func @main() {
    %0 = nl.distinct
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      %2 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %2 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
        %3 = nl.distinct_filter %0, (%arg1) : !nl.chunk<!storage.node_id>
        %4 = nl.get_out_edges(%3, {})
        nl.for %arg5, %arg6, %arg7, %arg8 in %4 : !nl.iter<!nl.chunk<!storage.node_id>, !nl.chunk<!storage.edge_id>, !nl.chunk<!storage.edge_type_id>, !nl.chunk<!storage.node_id>> {
          nl.output(%arg8) : !nl.chunk<!storage.node_id>
        }
      }
    }
    return
  }
}
