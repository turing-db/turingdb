// Generated nl-dialect lowering of remove_duplicates_limit.mlir (db dialect).
// Reproduce with: mlir -dump-lowered remove_duplicates_limit.mlir
// This is the DBLowering output; edit remove_duplicates_limit.mlir, not this file.
//
// DISTINCT ... LIMIT is the streaming path, not a fused top-K: both loops carry the
// `limit %1` handle (early-exit), and nl.limit_update charges the deduped survivor
// chunk %4 - so the loops stop once two distinct rows are emitted, not two scanned.
module {
  func.func @main() {
    %0 = nl.distinct
    %1 = nl.limit(2)
    %2 = nl.scan_nodes()
    nl.for %arg0 in %2 limit %1 {
      %3 = nl.get_out_edges(%arg0, {})
      nl.for %arg1, %arg2, %arg3, %arg4 in %3 limit %1 {
        %4 = nl.distinct_filter %0, (%arg4)
        nl.limit_update %1, %4
        nl.output(%4) limit %1
      }
    }
    return
  }
}
