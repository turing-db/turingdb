#ifndef TURINGDB_DIALECT
#define TURINGDB_DIALECT

include "mlir/IR/OpBase.td"

def DB : Dialect {
    let name = "db";
    let summary = "TuringDB dialect";
    let cppNamespace = "::mlir::db";

    // Removes the need to define custom printers for our custom types
    let useDefaultTypePrinterParser = 1;

    // Declare other functions: mlir::turing::db::f()
    let extraClassDeclaration = [{
        void registerTypes(); // Makes MLIR aware of our custom types
    }];

}

def ConstantThroughOperands : NativeOpTrait<"ConstantThroughOperands">;

// Base, from which all TuringDB dialects are derived
class TuringOp<string mnemonic, list<Trait> traits = []>
    : Op<DB, mnemonic, traits>;

class UnaryOp<string mnemonic, list<Trait> traits = []>
    : TuringOp<mnemonic, traits>;

// Arithmetic over two columns: constant when both operands are.
class BinaryOp<string mnemonic, list<Trait> traits = []>
    : TuringOp<mnemonic, !listconcat(traits, [ConstantThroughOperands])>;

#endif //TURINGDB_DIALECT
