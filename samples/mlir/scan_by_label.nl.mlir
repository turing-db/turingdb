// Generated nl-dialect lowering of scan_by_label.mlir (db dialect).
// Reproduce with: mlir -dump-lowered scan_by_label.mlir
// This is the DBLowering output; edit scan_by_label.mlir, not this file.
//
// No -graph needed to lower: unlike a property fetch, a label scan bakes no
// type from the schema - the label names are forwarded verbatim and resolved to
// LabelIDs only at execution. The scan opens one nl.for over its node chunk, and
// each chunk feeds nl.output, exactly as a plain nl.scan_nodes would.
module {
  func.func @main() {
    %0 = nl.scan_nodes_by_label(["Person"])
    nl.for %arg0 in %0 : !nl.iter<!nl.chunk<!storage.node_id>> {
      nl.output(%arg0) : !nl.chunk<!storage.node_id>
    }
    return
  }
}
