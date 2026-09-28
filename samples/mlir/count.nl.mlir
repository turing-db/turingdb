// Generated nl-dialect lowering of count.mlir (db dialect).
// Reproduce with: mlir -dump-lowered count.mlir
// This is the DBLowering output; edit count.mlir, not this file.
//
// db.count is a pipeline breaker like db.sort: a hoisted nl.count tally, an
// nl.count_update inside the scan loop that charges each chunk's rows, and - after
// the loop - an nl.count_result that materializes the single tally row as an
// unsigned i64 (!nl.chunk<ui64>), which a function-scope nl.output emits. It
// collapses to one row, so there is no emit loop.
module {
  func.func @main() {
    %0 = nl.count
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.count_update %0, %arg0 : !nl.chunk<!storage.node_id>
    }
    %2 = nl.count_result(%0) : !nl.chunk<ui64>
    nl.output(%2) : !nl.chunk<ui64>
    return
  }
}
