#ifndef TURINGDB_DIALECT
#define TURINGDB_DIALECT

include "mlir/IR/OpBase.td"

def TuringDB : Dialect {
    let name = "turing";
    let summary = "TuringDB dialect";
    let cppNamespace = "::mlir::turing";
    let useDefaultTypePrinterParser = 1;

    // Declare other functions: mlir::turing::TuringDB::f()
    let extraClassDeclaration = [{
        void registerTypes();
    }];

}

// Base, from which all TuringDB dialects are derived
class TuringOp<string mnemonic, list<Trait> traits = []>
    : Op<TuringDB, mnemonic, traits>;

#endif //TURINGDB_DIALECT
