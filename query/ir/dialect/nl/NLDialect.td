#ifndef TURINGDB_NL_DIALECT
#define TURINGDB_NL_DIALECT

include "mlir/IR/OpBase.td"

def NL : Dialect {
    let name = "nl";
    let summary = "TuringDB nested-loop dialect";
    let description = [{
        Imperative lowering target of the db dialect. Where db expresses
        whole-column dataflow, nl makes the execution explicit: database
        iterators driven by for loops that produce one set of chunks per step.
    }];
    let cppNamespace = "::mlir::nl";

    // Removes the need to define custom printers for our custom types
    let useDefaultTypePrinterParser = 1;

    // Declare other functions: mlir::nl::f()
    let extraClassDeclaration = [{
        void registerTypes(); // Makes MLIR aware of our custom types
    }];

}

def ConstantThroughOperands : NativeOpTrait<"ConstantThroughOperands">;

// Base, from which all nested-loop dialect ops are derived
class NLOp<string mnemonic, list<Trait> traits = []>
    : Op<NL, mnemonic, traits>;

#endif //TURINGDB_NL_DIALECT
