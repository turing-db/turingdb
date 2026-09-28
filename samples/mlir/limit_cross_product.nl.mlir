// Generated nl-dialect lowering of limit_cross_product.mlir (db dialect).
// Reproduce with: mlir -dump-lowered limit_cross_product.mlir
// This is the DBLowering output; edit limit_cross_product.mlir, not this file.
module {
  func.func @main() {
    %0 = nl.limit(5)
    %1 = nl.scan_nodes()
    nl.for %arg0 in %1 limit %0 {
      %2 = nl.scan_nodes()
      nl.for %arg1 in %2 limit %0 {
        %3 = nl.cross_product{%arg0} {%arg1}
        nl.for %arg2, %arg3 in %3 limit %0 {
          nl.limit_update %0, %arg2
          nl.output(%arg2, %arg3) limit %0
        }
      }
    }
    return
  }
}
