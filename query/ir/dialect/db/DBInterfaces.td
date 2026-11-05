#ifndef TURINGDB_INTERFACES
#define TURINGDB_INTERFACES

include "mlir/IR/OpBase.td"

// An op keeping the rows at some positions of its input, which a filter cannot move across
def KeepsRowsByPosition : NativeOpTrait<"KeepsRowsByPosition">;

def ColumnCarrierOpInterface : OpInterface<"ColumnCarrierOpInterface"> {
  let cppNamespace = "::mlir::db";

  let description = [{
    An op some of whose results, or arguments of whose regions, carry columns: their rows
    may be filtered, repeated, reordered or padded with null, but each value one holds is a
    value of a column it carries.
  }];

  let methods = [
    InterfaceMethod<
      "Fill `carrySet` with the run of operands the op carries row for row into a run of its results; false where it carries none",
      "bool", "getCarrySet",
      (ins "::mlir::db::CarrySet&":$carrySet), [{}],
      [{ return false; }]>,
    InterfaceMethod<
      "The column each row of `result` holds the value of, at the input row the row comes from; null where the op makes the result",
      "::mlir::Value", "getRowSource",
      (ins "::mlir::OpResult":$result), [{}],
      [{ return ::mlir::db::carrySetRowSource($_op.getOperation(), result); }]>,
    InterfaceMethod<
      "Append the columns `value`, a result of the op or an argument of one of its regions, carries; none where the op makes it",
      "void", "collectCarriedColumns",
      (ins "::mlir::Value":$value, "::llvm::SmallVectorImpl<::mlir::Value>&":$carried), [{}],
      [{ ::mlir::db::collectRowSource(value, carried); }]>,
    InterfaceMethod<
      "Fill `carrySet` with the run of operands the op carries into `region` as a run of its arguments; false where it carries none",
      "bool", "getRegionCarrySet",
      (ins "::mlir::Region&":$region, "::mlir::db::RegionCarrySet&":$carrySet), [{}],
      [{ return false; }]>,
    InterfaceMethod<
      "Set `resultOffset` to the result the first column `region` yields comes out as, each next one coming out as the next result with the rows the region left it; false where the op pads or merges what its regions yield",
      "bool", "getYieldedResultOffset",
      (ins "::mlir::Region&":$region, "size_t&":$resultOffset), [{}],
      [{ return false; }]>
  ];
}

#endif
