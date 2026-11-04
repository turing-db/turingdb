#ifndef TURINGDB_INTERFACES
#define TURINGDB_INTERFACES

include "mlir/IR/OpBase.td"

def ColumnCarrierOpInterface : OpInterface<"ColumnCarrierOpInterface"> {
  let cppNamespace = "::mlir::db";

  let description = [{
    An op some of whose results, or arguments of whose regions, carry columns: their rows
    may be filtered, repeated, reordered or padded with null, but each value one holds is a
    value of a column it carries.
  }];

  let methods = [
    InterfaceMethod<
      "Append the columns `value`, a result of the op or an argument of one of its regions, carries; none where the op makes it",
      "void", "collectCarriedColumns",
      (ins "::mlir::Value":$value, "::llvm::SmallVectorImpl<::mlir::Value>&":$carried)>
  ];
}

#endif
