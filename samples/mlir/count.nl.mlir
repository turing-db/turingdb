// Generated nl-dialect lowering of count.mlir (db dialect).
// Reproduce with: mlir -dump-lowered count.mlir
// This is the DBLowering output; edit count.mlir, not this file.
//
// db.count is a pipeline breaker like db.sort: a hoisted nl.count tally, an
// nl.count_update inside the scan loop that charges each chunk's rows, and - after
// the loop - an nl.count_result source iterator whose nl.for emits the single
// tally row as a nullable (always-present) i64.
module {
  func.func @main() {
    %0 = nl.count
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.count_update %0, %arg0 : !nl.chunk<!storage.node_id>
    }
    %2 = nl.count_result(%0) : !nl.iter<!nl.chunk<!storage.nullable<i64>>>
    nl.for %arg0 in %2 : !nl.iter<!nl.chunk<!storage.nullable<i64>>> {
      nl.output(%arg0) : !nl.chunk<!storage.nullable<i64>>
    }
    return
  }
}
