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
      [{ return false; }]>,
    InterfaceMethod<
      "The operands of the carry set, as a range that keeps the operand segment holding them sized to match",
      "::mlir::MutableOperandRange", "getCarrySetMutable",
      (ins), [{}],
      [{ return ::mlir::db::unsegmentedCarrySet($_op.getOperation()); }]>,
    InterfaceMethod<
      "The operands of the carry set into the op's regions, one run every region shares, as a range that keeps the operand segment holding them sized to match",
      "::mlir::MutableOperandRange", "getRegionCarrySetMutable",
      (ins), [{}],
      [{ return ::mlir::db::unsegmentedRegionCarrySet($_op.getOperation()); }]>,
    InterfaceMethod<
      "Whether `region` may forward columns it computes to every region after it, each of which sees a subset of its rows and takes the columns of every forwarding region before it, in region order, as its leading arguments",
      "bool", "forwardsColumns",
      (ins "::mlir::Region&":$region), [{}],
      [{ return false; }]>,
    InterfaceMethod<
      "The columns `region`, one that forwards columns, forwards to the regions after it",
      "::mlir::MutableOperandRange", "getForwardedColumnsMutable",
      (ins "::mlir::Region&":$region), [{}],
      [{ return ::mlir::MutableOperandRange($_op.getOperation(), 0, 0); }]>
  ];
}

def EdgeTypesOutputOpInterface : OpInterface<"EdgeTypesOutputOpInterface"> {
  let cppNamespace = "::mlir::db";

  let description = [{
    An op outputting, beside a column of edges, the type of each of those edges: the column a
    db.get_edge_types of the edges would read. A scan or a hop outputs its `etypes` beside its
    `eids`.
  }];

  let methods = [
    InterfaceMethod<
      "The result holding the type of each edge `edges` holds, row for row; null where the op outputs none beside it",
      "::mlir::Value", "getEdgeTypesOf",
      (ins "::mlir::OpResult":$edges), [{}],
      [{
        if (edges != $_op.getEids()) {
            return {};
        }

        return $_op.getEtypes();
      }]>
  ];
}

#endif
