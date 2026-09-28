// Generated nl-dialect lowering of sort_topk.mlir (db dialect).
// Reproduce with: mlir -dump-lowered sort_topk.mlir
// This is the DBLowering output; edit sort_topk.mlir, not this file.
//
// Note the ORDER BY ... LIMIT 2 fused away: no nl.limit / nl.limit_update /
// nl.limit_truncate. The bound lives on nl.sort_buffer as `limit 2`, so the
// accumulator keeps only the best 2 rows, and the scan loop is unbounded because
// top-K must still scan every row.
module {
  func.func @main() {
    %0 = nl.sort_buffer keys [0] ascending [false] limit 2
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.sort_collect %0, (%arg0) : !nl.chunk<!storage.node_id>
    }
    %2 = nl.sort(%0) : !nl.iter<!nl.chunk<!storage.node_id>>
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.output(%arg0) : !nl.chunk<!storage.node_id>
    }
    return
  }
}
