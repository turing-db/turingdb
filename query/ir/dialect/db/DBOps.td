include "mlir/IR/OpBase.td"
include "mlir/IR/BuiltinTypes.td"
include "mlir/Interfaces/SideEffectInterfaces.td"

// -- Dialect declaration --
def DB : Dialect {
  let name = "db";
  let summary = "TuringDB dialect";
  let cppNamespace = "::mlir::db";
}

// -- Base class for all ops in this dialect --
class My_Op<string mnemonic, list<Trait> traits = []>
    : Op<DB, mnemonic, traits>;

// -- A simple add op --
def AddOp : My_Op<"add", [Pure]> {
  let summary = "Integer addition";
  let arguments = (ins I64:$lhs, I64:$rhs);
  let results   = (outs I64:$result);
  let assemblyFormat = "$lhs `,` $rhs attr-dict `:` type($lhs)";
}
