include "mlir/IR/OpBase.td"
include "mlir/IR/BuiltinTypes.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

// Dialect declaration
def TuringDB : Dialect {
  let name = "turing";
  let summary = "TuringDB dialect";
  let cppNamespace = "::mlir::turing";
}

// Base, from which all TuringDB dialects are derived
class TuringOp<string mnemonic, list<Trait> traits = []>
    : Op<TuringDB, mnemonic, traits>;

/**
* ScanNodes
* Pure: this operation has no side effects
*/
def ScanNodes : TuringOp<"ScanNodes", [Pure]> {
  let summary = "Produce all nodes in the graph";
  let results   = (outs I64:$result);
}
