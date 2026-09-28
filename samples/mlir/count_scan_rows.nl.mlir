// Generated nl-dialect lowering of count_scan_rows.mlir (db dialect).
// Reproduce with: mlir -dump-lowered count_scan_rows.mlir
// This is the DBLowering output; edit count_scan_rows.mlir, not this file.
//
// A count read from the graph's node counts needs no loop and no accumulator: one
// nl.count_scan_rows, hoisted to the top of the entry block the way a constant is,
// materializes the single tally row as an unsigned i64 (!nl.chunk<ui64>), which a
// function-scope nl.output emits. Compare count.nl.mlir, which reaches the same chunk
// through a scan loop, a tally handle and an emit.
module {
  func.func @main() {
    %0 = nl.count_scan_rows([["Person"], ["Interest"]])
    nl.output(%0) : !nl.chunk<ui64>
    return
  }
}
